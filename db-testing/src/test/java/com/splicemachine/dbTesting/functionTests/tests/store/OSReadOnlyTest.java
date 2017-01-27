/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.dbTesting.functionTests.tests.store;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.functionTests.util.PrivilegedFileOpsForTests;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.JDBCDataSource;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Simulates running Derby on a read-only media, and makes sure Derby gives a
 * reasonable error message when the user tries to insert data into the
 * read-only database.
 */
public class OSReadOnlyTest extends BaseJDBCTestCase{

    public OSReadOnlyTest(String name) {
        super(name);
    }
    
    private static Test newCleanDatabase(TestSuite s) {
        return new CleanDatabaseTestSetup(s) {
        /**
         * Creates the database objects used in the test cases.
         *
         * @throws SQLException
         */
            /**
             * Creates the tables used in the test cases.
             * @exception SQLException if a database error occurs
             */
            protected void decorateSQL(Statement stmt) throws SQLException
            {
                getConnection();

                // create a table with some data
                stmt.executeUpdate(
                    "CREATE TABLE foo (a int, b char(100))");
                stmt.execute("insert into foo values (1, 'hello world')");
                stmt.execute("insert into foo values (2, 'happy world')");
                stmt.execute("insert into foo values (3, 'sad world')");
                stmt.execute("insert into foo values (4, 'crazy world')");
                for (int i=0 ; i<7 ; i++)
                    stmt.execute("insert into foo select * from foo");
                stmt.execute("create index fooi on foo(a, b)");
            }
        };
    }

    protected static Test baseSuite(String name) 
    {
        TestSuite readonly = new TestSuite("OSReadOnly");
        TestSuite suite = new TestSuite(name);
        readonly.addTestSuite(OSReadOnlyTest.class);
        suite.addTest(TestConfiguration.singleUseDatabaseDecorator(newCleanDatabase(readonly)));
        
        return suite;
    }

    public static Test suite() 
    {
        TestSuite suite = new TestSuite("OSReadOnlyTest");
        suite.addTest(baseSuite("OSReadOnlyTest:embedded"));
        suite.addTest(TestConfiguration
            .clientServerDecorator(baseSuite("OSReadOnlyTest:client")));
        return suite;
    }
    
    /**
     * Test that if we make the files comprising the database read-only
     * on OS level, the database reacts as if it's in 'ReadOnly' mode
     */
    public void testOSReadOnly() throws Exception {
        // start with some simple checks
        setAutoCommit(false);
        Statement stmt = createStatement();
        JDBC.assertFullResultSet(stmt.executeQuery(
            "select count(*) from foo"), new String[][] {{"512"}});
        stmt.executeUpdate("delete from foo where a = 1");
        JDBC.assertFullResultSet(stmt.executeQuery(
            "select count(*) from foo"), new String[][] {{"384"}});
        rollback();
        JDBC.assertFullResultSet(stmt.executeQuery(
            "select count(*) from foo"), new String[][] {{"512"}});
        stmt.executeUpdate("insert into foo select * from foo where a = 1");
        JDBC.assertFullResultSet(stmt.executeQuery(
            "select count(*) from foo"), new String[][] {{"640"}});
        commit();
        stmt.executeUpdate("delete from foo where a = 1");
        JDBC.assertFullResultSet(stmt.executeQuery(
            "select count(*) from foo"), new String[][] {{"384"}});
        rollback();
        JDBC.assertFullResultSet(stmt.executeQuery(
            "select count(*) from foo"), new String[][] {{"640"}});
        setAutoCommit(false);
        
        TestConfiguration.getCurrent().shutdownDatabase();
        
        // so far, we were just playing. Now for the test.
        String phDbName = getPhysicalDbName();
        // copy the database to one called 'readOnly'
        moveDatabaseOnOS(phDbName, "readOnly");
        // change filePermissions on readOnly, to readonly.
        changeFilePermissions("readOnly");
        createDummyLockFile("readOnly");
        
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, 
            "databaseName", "singleUse/readOnly");
        assertReadDB(ds);
        assertExpectedInsertBehaviour(ds, false, 10, "will fail");
        shutdownDB(ds);
        
        // copy the database to one called 'readWrite' 
        // this will have the default read/write permissions upon
        // copying
        moveDatabaseOnOS("readOnly", "readWrite");
        ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, "databaseName", "singleUse/readWrite");
        assertReadDB(ds);
        assertExpectedInsertBehaviour(ds, true, 20, "will go in");
        shutdownDB(ds);
        
        // do it again...
        moveDatabaseOnOS("readWrite", "readOnly2");
        // change filePermissions on readOnly, to readonly.
        changeFilePermissions("readOnly2");
        createDummyLockFile("readOnly2");
        
        ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, 
            "databaseName", "singleUse/readOnly2");
        assertReadDB(ds);
        assertExpectedInsertBehaviour(ds, false, 30, "will also fail");
        shutdownDB(ds);
        
        // testharness will try to remove the original db; put it back
        moveDatabaseOnOS("readOnly2", phDbName);
    }
    
    /*
     * figure out the physical database name, we want to manipulate
     * the actual files on the OS.
     */
    private String getPhysicalDbName() {
        String pdbName =TestConfiguration.getCurrent().getJDBCUrl();
        if (pdbName != null)
            pdbName=pdbName.substring(pdbName.lastIndexOf("oneuse"),pdbName.length());
        else {
            // with JSR169, we don't *have* a protocol, and so, no url, and so
            // we'll have had a null.
            // But we know the name of the db is something like system/singleUse/oneuse#
            // So, let's see if we can look it up, if everything's been properly
            // cleaned, there should be just 1...
            pdbName = (String) AccessController.doPrivileged(new java.security.PrivilegedAction() {
                String filesep = getSystemProperty("file.separator");
                public Object run() {
                    File dbdir = new File("system" + filesep + "singleUse");
                    String[] list = dbdir.list();
                    // Some JVMs return null for File.list() when the directory is empty
                    if( list != null)
                    {
                        if(list.length > 1)
                        {
                            for( int i = 0; i < list.length; i++ )
                            {
                                if(list[i].indexOf("oneuse")<0)
                                    continue;
                                else
                                {
                                    return list[i];
                                }
                            }
                            // give up trying to be smart, assume it's 0
                            return "oneuse0";
                        }
                        else
                            return list[0];
                    }
                    return null;
                }
            });
            
        }
        return pdbName;
    }
    
    private void shutdownDB(DataSource ds) throws SQLException {
        JDBCDataSource.setBeanProperty(
            ds, "ConnectionAttributes", "shutdown=true");
        try {
            ds.getConnection();
            fail("expected an sqlexception 08006");
        } catch (SQLException sqle) {
            assertSQLState("08006", sqle);
        }        
    }
    
    private void assertReadDB(DataSource ds) throws SQLException {
        Connection con = ds.getConnection();
        Statement stmt2 = con.createStatement();
        JDBC.assertFullResultSet(
            stmt2.executeQuery("select count(*) from foo where a=1"),
            new String [][] {{"256"}});
        JDBC.assertFullResultSet(
            stmt2.executeQuery("select count(*) from foo where a=2"),
            new String [][] {{"128"}});
        JDBC.assertFullResultSet(
            stmt2.executeQuery("select count(*) from foo where a=1 and b='hello world'"),
            new String [][] {{"256"}});
        stmt2.close();
        con.close();
    }
    
    private void assertExpectedInsertBehaviour(
            DataSource ds, boolean expectedSuccess, 
            int insertIntValue, String insertStringValue) 
    throws SQLException {
        Connection con = ds.getConnection();
        Statement stmt = con.createStatement();
        if (expectedSuccess)
        {
            stmt.executeUpdate("insert into foo values (" +
                insertIntValue + ", '" + insertStringValue + "')");
            assertTrue(stmt.getUpdateCount() == 1);
            JDBC.assertFullResultSet(
                stmt.executeQuery("select count(*) from foo where a=" +
                    insertIntValue), new String [][] {{"1"}});
        }
        else {
            try {
                stmt.executeUpdate("insert into foo values (" +
                    insertIntValue + ", '" + insertStringValue + "')");
                fail("expected an error indicating the db is readonly");
            } catch (SQLException sqle) {
                if (!(sqle.getSQLState().equals("25502") || 
                        // on iseries / OS400 machines, when file/os 
                        // permissions are off, we may get error 40XD1 instead
                        sqle.getSQLState().equals("40XD1")))
                    fail("unexpected sqlstate; expected 25502 or 40XD1, got: " + sqle.getSQLState());
            }
        }
        stmt.close();
        con.close();
    }

    /**
     * Moves the database from one location to another location.
     *
     * @param fromwhere source directory
     * @param todir destination directory
     * @throws IOException if the copy fails
     */
    private void moveDatabaseOnOS(String fromwhere, String todir)
            throws IOException {
        File from_dir = constructDbPath(fromwhere);
        File to_dir = constructDbPath(todir);

        PrivilegedFileOpsForTests.copy(from_dir, to_dir);
        assertDirectoryDeleted(from_dir);
    }

    /**
     * Creates a dummy database lock file if one doesn't exist, and sets the
     * lock file to read-only.
     * <p>
     * This method is a work-around for the problem that Java cannot make a file
     * writable before Java 6.
     *
     * @param dbDir the database directory where the lock file belongs
     */
    private void createDummyLockFile(String dbDir) {
        final File f = new File(constructDbPath(dbDir), "db.lck");
        AccessController.doPrivileged(new PrivilegedAction() {

            public Object run() {
                if (!f.exists()) {
                    try {
                        FileOutputStream fos = new FileOutputStream(f);
                        // Just write a dummy byte.
                        fos.write(12);
                        fos.close();
                    } catch (IOException fnfe) {
                        // Ignore
                    }
                }
                f.setReadOnly();
                return null;
            }
        });
    }

    public void changeFilePermissions(String dir) {
        File dir_to_change = constructDbPath(dir);
        assertTrue("Failed to change files in " + dir_to_change + " to ReadOnly",
            changeDirectoryToReadOnly(dir_to_change));
    }

    /**
     * Constructs the path to the database base directory.
     *
     * @param relDbDirName the database name (relative)
     * @return The path to the database.
     */
    private File constructDbPath(String relDbDirName) {
        // Example: "readOnly" -> "<user.dir>/system/singleUse/readOnly"
        File f = new File(getSystemProperty("user.dir"), "system");
        f = new File(f, "singleUse");
        return new File(f, relDbDirName);
    }

    /**
     * Change all of the files in a directory and its subdirectories
     * to read only.
     * @param directory the directory File handle to start recursing from.
     * @return <code>true</code> for success, <code>false</code> otherwise
     */
    public static boolean changeDirectoryToReadOnly( File directory )
    {
        if( null == directory )
            return false;
        final File sdirectory = directory;

        Boolean b = (Boolean)AccessController.doPrivileged(
            new java.security.PrivilegedAction() {
                public Object run() {
                    // set fail to true to start with; unless it works, we
                    // want to specifically set the value.
                    boolean success = true;
                    if( !sdirectory.isDirectory() )
                        success = false;
                    String[] list = sdirectory.list();
                    // Some JVMs return null for File.list() when the directory is empty
                    if( list != null )
                    {
                        for( int i = 0; i < list.length; i++ )
                        {
                            File entry = new File( sdirectory, list[i] );
                            if( entry.isDirectory() )
                            {
                                if( !changeDirectoryToReadOnly(entry) )
                                    success = false;
                            }
                            else {
                                if( !entry.setReadOnly() )
                                    success = false;
                            }
                        }
                    }
                    // Before Java 6 we cannot make the directory writable
                    // again, which means we cannot delete the directory and
                    // its content...
                    //success &= sdirectory.setReadOnly();
                    return new Boolean(success);
                }
            });        
        if (b.booleanValue())
        {
            return true;
        }
        else return false;
    }
}
