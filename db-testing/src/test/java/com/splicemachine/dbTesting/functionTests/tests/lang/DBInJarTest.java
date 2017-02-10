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

package com.splicemachine.dbTesting.functionTests.tests.lang;

import java.io.File;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.SecurityManagerSetup;


public class DBInJarTest extends BaseJDBCTestCase {

    public DBInJarTest(String name) {
        super(name);

    }


    /**
     * Create and connect to a database in a jar.
     * @throws SQLException
     */
    public void testConnectDBInJar() throws SQLException
    {
        //      Create database to be jarred up.

        Connection beforejarconn = DriverManager.getConnection("jdbc:splice:testjardb;create=true");
        Statement bjstmt = beforejarconn.createStatement();
        bjstmt.executeUpdate("CREATE TABLE TAB (I INT)");
        bjstmt.executeUpdate("INSERT INTO TAB VALUES(1)");
        shutdownDB("jdbc:splice:testjardb;shutdown=true");
        Statement stmt = createStatement();

        stmt.executeUpdate("CALL CREATEARCHIVE('testjardb.jar', 'testjardb','testjardb')");
        Connection jarconn = DriverManager.getConnection("jdbc:splice:jar:(testjardb.jar)testjardb");
        Statement s = jarconn.createStatement();

        // try to read from a table.
        ResultSet rs = s.executeQuery("SELECT * from TAB");
        JDBC.assertSingleValueResultSet(rs, "1");

        // Try dbmetadata call. DERBY-3546
        rs = jarconn.getMetaData().getSchemas();
        String[][] expectedRows = {{"SPLICE",null},
                {"NULLID",null},
                {"SQLJ",null},
                {"SYS",null},
                {"SYSCAT",null},
                {"SYSCS_DIAG",null},
                {"SYSCS_UTIL",null},
                {"SYSFUN",null},
                {"SYSIBM",null},
                {"SYSPROC",null},
                {"SYSSTAT",null}};
        JDBC.assertFullResultSet(rs, expectedRows);
        shutdownDB("jdbc:splice:jar:(testjardb.jar)testjardb;shutdown=true");

        // cleanup databases
        File jarreddb = new File(System.getProperty("derby.system.home") + "/testjardb.jar");
        assertTrue("failed deleting " + jarreddb.getPath(),jarreddb.delete());
        removeDirectory(new File(System.getProperty("derby.system.home") + "/testjardb" ));
    }


    private void shutdownDB(String url) {
        try {
            DriverManager.getConnection(url);
            fail("Expected exception on shutdown");
        } catch (SQLException se) {
            assertSQLState("08006", se);
        }
    }

    /**
     * Test various queries that use a hash table that may be spilled to disk
     * if it grows too big. Regression test case for DERBY-2354.
     */
    public void testSpillHashToDisk() throws SQLException {
        createDerby2354Database();

        Connection jarConn =
                DriverManager.getConnection("jdbc:splice:jar:(d2354db.jar)d2354db");

        Statement stmt = jarConn.createStatement();

        // The following statement used to fail with "Feature not implemented"
        // or "Container was opened in read-only mode" before DERBY-2354. It
        // only fails if the hash table used for duplicate elimination spills
        // to disk, which happens if the hash table gets bigger than 1% of the
        // total amount of memory allocated to the JVM. This means it won't
        // expose the bug if the JVM runs with very high memory settings (but
        // it has been tested with 1 GB heap size and then it did spill to
        // disk).
        JDBC.assertDrainResults(
                stmt.executeQuery("select distinct x from d2354"),
                40000);

        // Hash joins have the same problem. Force the big table to be used as
        // the inner table in the hash join.
        JDBC.assertEmpty(stmt.executeQuery(
                "select * from --DERBY-PROPERTIES joinOrder = FIXED\n" +
                        "sysibm.sysdummy1 t1(x),\n" +
                        "d2354 t2 --DERBY-PROPERTIES joinStrategy = HASH\n" +
                        "where t1.x = t2.x"));

        // Scrollable result sets keep the rows they've visited in a hash
        // table, so they may also need to store data on disk temporarily.
        Statement scrollStmt = jarConn.createStatement(
                ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
        JDBC.assertDrainResults(
                scrollStmt.executeQuery("select * from d2354"),
                40000);

        stmt.close();
        scrollStmt.close();
        jarConn.close();

        // Cleanup. Shut down the database and delete it.
        shutdownDB("jdbc:splice:jar:(d2354db.jar)d2354db;shutdown=true");
        removeFiles(new String[] {
                System.getProperty("derby.system.home") + "/d2354db.jar"
        });
    }

    /**
     * Create a database in a jar for use in {@code testSpillHashToDisk}.
     */
    private void createDerby2354Database() throws SQLException {
        // First create an ordinary database with a table.
        Connection conn =
                DriverManager.getConnection("jdbc:splice:d2354db;create=true");
        conn.setAutoCommit(false);
        Statement s = conn.createStatement();
        s.execute("create table d2354 (x varchar(100))");
        s.close();

        // Insert 40000 unique values into the table. The values should be
        // unique so that they all occupy an entry in the hash table used by
        // the DISTINCT query in the test, and thereby increase the likelihood
        // of spilling to disk.
        PreparedStatement insert =
                conn.prepareStatement(
                        "insert into d2354 values ? || " +
                                "'some extra data to increase the size of the table'");
        for (int i = 0; i < 40000; i++) {
            insert.setInt(1, i);
            insert.executeUpdate();
        }
        insert.close();

        conn.commit();
        conn.close();

        // Shut down the database and archive it in a jar file.
        shutdownDB("jdbc:splice:d2354db;shutdown=true");

        createStatement().execute(
                "CALL CREATEARCHIVE('d2354db.jar', 'd2354db', 'd2354db')");

        // Clean up the original database directory. We don't need it anymore
        // now that we have archived it in a jar file.
        removeDirectory(
                new File(System.getProperty("derby.system.home") + "/d2354db"));
    }

    protected static Test baseSuite(String name) {
        TestSuite suite = new TestSuite(name);
        suite.addTestSuite(DBInJarTest.class);
        // Don't run with security manager, we need access to user.dir to archive
        // the database.
        return new CleanDatabaseTestSetup(SecurityManagerSetup.noSecurityManager(suite))
        {
            /**
             * Creates the procedure used in the test cases.
             * @exception SQLException if a database error occurs
             */
            protected void decorateSQL(Statement stmt) throws SQLException
            {
                stmt.execute("create procedure CREATEARCHIVE(jarName VARCHAR(20)" +
                        " , path VARCHAR(20), dbName VARCHAR(20))" +
                        " LANGUAGE JAVA PARAMETER STYLE JAVA" +
                        " NO SQL" +
                        " EXTERNAL NAME 'com.splicemachine.dbTesting.functionTests.tests.lang.dbjarUtil.createArchive'");


            }
        };
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("DBInJarTest");
        suite.addTest(baseSuite("DBInJarTest:embedded"));
        return suite;

    }
}
