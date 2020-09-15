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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.dbTesting.functionTests.tests.jdbcapi;

import java.io.File;
import java.lang.reflect.Method;
import java.security.AccessController;
import java.sql.SQLException;

import javax.sql.ConnectionPoolDataSource;
import javax.sql.XADataSource;

import junit.extensions.TestSetup;
import org.junit.Assert;
import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.BaseTestCase;
import com.splicemachine.dbTesting.junit.J2EEDataSource;
import com.splicemachine.dbTesting.junit.JDBCDataSource;
import com.splicemachine.dbTesting.junit.TestConfiguration;

public class PoolXADSCreateShutdownDBTest extends BaseJDBCTestCase {

    static final String[] ADDITIONAL_DBS = {
        "dscreateconatdb1",
        "dscreateshutdowndb1", 
        "dscreateshutdowndb2",
        "conflict1",
        "conflict2",
        "conflict3",
        "conflict4",
        "conflict5",
        "conflict6",
        "conflict7"
    };
    
    static String DBNotFoundState;

	private String ShutdownState = "08006";
    
    public PoolXADSCreateShutdownDBTest(String name) {
        super(name);
    }

    public static Test suite() 
    {
        TestSuite suite = new TestSuite("PoolXADSCreateShutdownTest"); 
        Test test = TestConfiguration.defaultSuite(PoolXADSCreateShutdownDBTest.class);        
        //Test test = TestConfiguration.clientServerSuite(DSCreateShutdownDBTest.class);
        suite.addTest(test);
        
        TestSetup setup = TestConfiguration.singleUseDatabaseDecorator(suite);
        // we need a couple extra databases to test they get created
        for (int i = 0; i < ADDITIONAL_DBS.length; i++)
        {
            setup = TestConfiguration.additionalDatabaseDecorator(setup,
                "emb" + ADDITIONAL_DBS[i]);
            setup = TestConfiguration.additionalDatabaseDecorator(setup,
                "srv" + ADDITIONAL_DBS[i]);
        }
    
        return suite;
    }
    
    public void tearDown() throws Exception {
        // attempt to get rid of any databases. 
        // only 5 dbs (in addition to the defaultdb) should actually get
        // created, but just in case...
        AccessController.doPrivileged(new java.security.PrivilegedAction() {
            public Object run() {
                for (int i=0 ; i < ADDITIONAL_DBS.length ; i++)
                {   
                    removeDatabase("emb" + ADDITIONAL_DBS[i]);
                    removeDatabase("srv" + ADDITIONAL_DBS[i]);
                } 
                return null;
            }
            
            void removeDatabase(String dbName)
            {
                //TestConfiguration config = TestConfiguration.getCurrent();
                dbName = dbName.replace('/', File.separatorChar);
                String dsh = BaseTestCase.getSystemProperty("derby.system.home");
                if (dsh == null) {
                    fail("not implemented");
                } else {
                    dbName = dsh + File.separator + dbName;
                }
                removeDirectory(dbName);
            }

            void removeDirectory(String path)
            {
                final File dir = new File(path);
                removeDir(dir);
            }

            private void removeDir(File dir) {
                
                // Check if anything to do!
                // Database may not have been created.
                if (!dir.exists())
                    return;

                String[] list = dir.list();

                // Some JVMs return null for File.list() when the
                // directory is empty.
                if (list != null) {
                    for (int i = 0; i < list.length; i++) {
                        File entry = new File(dir, list[i]);

                        if (entry.isDirectory()) {
                            removeDir(entry);
                        } else {
                            entry.delete();
                            //assertTrue(entry.getPath(), entry.delete());
                        }
                    }
                }
                dir.delete();
                //assertTrue(dir.getPath(), dir.delete());
            }
        });
        super.tearDown();
    }

    public void testPoolDS() throws SQLException {
        ConnectionPoolDataSource ds = 
            J2EEDataSource.getConnectionPoolDataSource();
        doCreateAndShutdown(ds);
    }
    
    public void testXADS() throws SQLException {
        XADataSource ds = J2EEDataSource.getXADataSource();
        doCreateAndShutdown(ds);
    }
    
    public void doCreateAndShutdown(Object ds) throws SQLException {
        
        if (usingEmbedded())
            DBNotFoundState = "XJ004";
        else
            DBNotFoundState = "08004"; 
             
   
        
        // first play with default db, which is already created.
        String dbName = 
            TestConfiguration.getCurrent().getDefaultDatabaseName();
        // just check that we really access the database
        assertUpdateCount(createStatement(), 0, "set schema SPLICE");
   
        // check that first the value is null
        assertGetNull(ds, dbName);
        // check that we can set & that when set we can get
        // doesn't actually open connections so a little silly.
        assertSetAndGet(ds, dbName, "shutdownDatabase", "shutdown");
        assertSetAndGet(ds, dbName, "createDatabase", "create");
        // set to an invalid value, should get ignored
        assertNotSetAndGet(ds, dbName, "shutdownDatabase", "boo");
        assertNotSetAndGet(ds, dbName, "createDatabase", "boo");
        assertNotSetAndGet(ds, dbName, "shutdownDatabase", "false");
        assertNotSetAndGet(ds, dbName, "createDatabase", "false");
        
        assertReset(ds, dbName);
        clearBeanProperties(ds);
        // check that create using ConnAttributes works
        assertCreateUsingConnAttrsOK(
            ds, composeDatabaseName(ADDITIONAL_DBS[0]));
        clearBeanProperties(ds);
        // check that shutting down using Attributes works
        assertShutdownUsingConnAttrsOK(ds, dbName);
        clearBeanProperties(ds);
        // now, actually create using setCreateDB, and shutdown a database
        // first ensure it's not there yet
        dbName = composeDatabaseName(ADDITIONAL_DBS[1]);
        
        assertNoDB(ds, dbName);
        // straightforward create and shutdown
        clearBeanProperties(ds);
        assertPositive(ds, dbName);
        clearBeanProperties(ds);
        // what happens when you combine set*Database and
        // matching connection attribute? (should work)
        dbName = composeDatabaseName(ADDITIONAL_DBS[2]);
        assertNoDB(ds, dbName);
        clearBeanProperties(ds);
        assertTwiceOK(ds, dbName);
        
        clearBeanProperties(ds);
        // the rest of the testing is on conflicted settings
        // the result is not defined, so a change in behavior does not 
        // necessarily indicate a bug, but may be relevant for existing apps
        // what happens when you combine create and shutdown connattr?
        // database does not get created.
        assertShutdownAndCreateConnAttr(DBNotFoundState, ds,  
            composeDatabaseName(ADDITIONAL_DBS[3]), 
            "shutdown=true;create=true");
        clearBeanProperties(ds);
        assertShutdownAndCreateConnAttr(DBNotFoundState, ds, 
            composeDatabaseName(ADDITIONAL_DBS[4]), 
            "create=true;shutdown=true");
        clearBeanProperties(ds);
        // and when you set both setShutdownDatabase and setCreateDatabase?
        // database does not get created
        assertConflictedSettersOK(ds, composeDatabaseName(ADDITIONAL_DBS[5]));
        clearBeanProperties(ds);
        // what happens when you combine set*Database and
        // opposing connection attributes? database does not get created. 
        assertConflictedSetterConnAttrOK(ds);
    }
    
    protected String composeDatabaseName(String dbName) {
        if (usingEmbedded())
            return "emb" + dbName;
        else 
            return "srv" + dbName;
    }
    
    protected void assertGetNull(Object ds, String dbName) throws SQLException {
        assertNull(getBeanProperty(ds, "shutdownDatabase"));
        assertNull(getBeanProperty(ds, "createDatabase"));
    }
    
    protected void assertSetAndGet(
        Object ds, String dbName, String propertyString, String setValue)
    throws SQLException {
        JDBCDataSource.setBeanProperty(ds, propertyString, setValue);
        assertEquals(setValue,getBeanProperty(ds, propertyString).toString());
    }
    
    protected void assertNotSetAndGet(
        Object ds, String dbName, String propertyString, String setValue)
    throws SQLException {
        JDBCDataSource.setBeanProperty(ds, propertyString, setValue);
        assertNull(getBeanProperty(ds, propertyString));
    }
    
    protected void assertReset(Object ds, String dbName) 
    throws SQLException {
        JDBCDataSource.setBeanProperty(ds, "createDatabase", "");
        assertNull(getBeanProperty(ds, "createDatabase"));
        JDBCDataSource.setBeanProperty(ds, "createDatabase", "create");
        assertEquals("create", getBeanProperty(ds, "createDatabase"));
        JDBCDataSource.setBeanProperty(ds, "createDatabase", "boo");
        assertNull(getBeanProperty(ds, "createDatabase"));
        JDBCDataSource.setBeanProperty(ds, "createDatabase", "create");
        assertEquals("create", getBeanProperty(ds, "createDatabase"));
        JDBCDataSource.setBeanProperty(ds, "createDatabase", "false");
        assertNull(getBeanProperty(ds, "createDatabase"));
        JDBCDataSource.setBeanProperty(ds, "createDatabase", "create");
        assertEquals("create", getBeanProperty(ds, "createDatabase"));
        JDBCDataSource.setBeanProperty(ds, "createDatabase", "");
        assertNull(getBeanProperty(ds, "createDatabase"));
        try { 
            JDBCDataSource.setBeanProperty(ds, "createDatabase", "");
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "");
        assertNull(getBeanProperty(ds, "shutdownDatabase"));
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        assertEquals("shutdown", getBeanProperty(ds, "shutdownDatabase"));
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "boo");
        assertNull(getBeanProperty(ds, "shutdownDatabase"));
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "false");
        assertNull(getBeanProperty(ds, "shutdownDatabase"));
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        assertEquals("shutdown", getBeanProperty(ds, "shutdownDatabase"));
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "");
        assertNull(getBeanProperty(ds, "shutdownDatabase"));
        try { 
            JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    public static Object getBeanProperty(Object ds, String propertyString)
    {
        String getterName = getGetterName(propertyString);

        // Base the type of the setter method from the value's class.

        Object retObject=null;
        try {
            Method getter = ds.getClass().getMethod(getterName, null);
            retObject = getter.invoke(ds, null);
        } catch (Exception e) {
            Assert.fail(e.getMessage());
        }
        return retObject;
    }

    private static String getGetterName(String attribute) {
        return "get" + Character.toUpperCase(attribute.charAt(0))
        + attribute.substring(1);
    }
    
    // if the connattr parameter is true, we set both setShutdownDatabase
    // and ConnectionAttribute shutdown=true.
    protected void assertShutdownUsingSetOK(
        Object ds, String dbName, boolean connAttr) throws SQLException {

        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(ds, "databaseName", dbName);
        if (connAttr)
            JDBCDataSource.setBeanProperty(
                ds, "ConnectionAttributes", "shutdown=true");
        assertDSConnectionFailed(ShutdownState, ds);
    }
    
    // for completeness' sake, test create=true conn attr.
    protected void assertCreateUsingConnAttrsOK(Object ds, String dbName)
    throws SQLException {
        JDBCDataSource.setBeanProperty(
                ds, "ConnectionAttributes", "create=true");
        JDBCDataSource.setBeanProperty(ds,"databaseName", dbName);
        assertUpdateCount(ds);
        JDBCDataSource.clearStringBeanProperty(ds, "ConnectionAttributes");
        assertShutdownUsingSetOK(ds, dbName, false);
    }
    
    protected void assertShutdownUsingConnAttrsOK(Object ds, String dbName)
    throws SQLException {
        JDBCDataSource.setBeanProperty(ds, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(
            ds, "ConnectionAttributes", "shutdown=true");
        assertDSConnectionFailed(ShutdownState, ds);
    }

    protected void assertShutdownAndCreateConnAttr(
        String expectedSQLState, Object ds, String dbName, String twoPropertyString)
    throws SQLException {
        JDBCDataSource.setBeanProperty(ds, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(
            ds, "ConnectionAttributes", twoPropertyString);
        assertDSConnectionFailed(expectedSQLState, ds);
    }
    
    protected void assertDSConnectionFailed(
        String expectedSQLState, Object ds) throws SQLException {
        try {
            if (ds instanceof javax.sql.ConnectionPoolDataSource)
                ((ConnectionPoolDataSource)ds).getPooledConnection();
            else
                ((XADataSource)ds).getXAConnection();
            fail("expected an sqlexception " + expectedSQLState);
        } catch (SQLException sqle) {
            assertSQLState(expectedSQLState, sqle);
        }
    }    
    
    protected void assertNoDB(Object ds, String dbName) throws SQLException {
        JDBCDataSource.setBeanProperty(ds, "databaseName", dbName);
        assertDSConnectionFailed(DBNotFoundState, ds);
    }
    
    protected void assertPositive(Object ds, String dbName) 
    throws SQLException {
        JDBCDataSource.setBeanProperty(ds, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(ds, "CreateDatabase", "create");
        // check that the db exists; execute an unnecessary, but harmless, stmt
        assertUpdateCount(ds); 
        clearBeanProperties(ds);
        assertShutdownUsingSetOK(ds, dbName, false);
    }

    protected void assertTwiceOK(Object ds, String dbName) throws SQLException{
        JDBCDataSource.setBeanProperty(ds, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(ds, "CreateDatabase", "create");
        JDBCDataSource.setBeanProperty(
            ds, "ConnectionAttributes", "create=true");
        // check that the db exists; execute an unnecessary, but harmless, stmt
        assertUpdateCount(ds);
        clearBeanProperties(ds);
        
        assertShutdownUsingSetOK(ds, dbName, true);
    }
    
    protected void assertConflictedSettersOK(Object ds, String dbName) 
    throws SQLException {
        JDBCDataSource.setBeanProperty(ds, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(ds, "CreateDatabase", "create");
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        try {
            if (ds instanceof javax.sql.ConnectionPoolDataSource)
                ((ConnectionPoolDataSource)ds).getPooledConnection();
            else
                ((XADataSource)ds).getXAConnection();
            fail("expected an sqlexception " + DBNotFoundState);
        } catch (SQLException se) {
            assertSQLState(DBNotFoundState, se);
        }
    }

    protected void assertConflictedSetterConnAttrOK(Object ds) 
    throws SQLException {
        assertConSetOK(ds, DBNotFoundState, 
            composeDatabaseName(ADDITIONAL_DBS[6]), 
            "shutdown=true", "CreateDatabase", "create");
        // with the new networkserver methods, this actually works...
        assertConSetOK(ds, DBNotFoundState, 
            composeDatabaseName(ADDITIONAL_DBS[7]),
            "create=true", "ShutdownDatabase", "shutdown");
        assertSetConOK(ds, DBNotFoundState, 
            composeDatabaseName(ADDITIONAL_DBS[8]), 
            "shutdown=true", "CreateDatabase", "create");
        // with the new networkserver methods, this actually works...
        assertSetConOK(ds, DBNotFoundState, 
            composeDatabaseName(ADDITIONAL_DBS[9]),
            "create=true", "ShutdownDatabase", "shutdown");
    }
    
    // first sets setCreate/ShutdownDB, then sets ConnectionAttributes
    protected void assertConSetOK(Object ds, String expectedSQLState, 
        String dbName, String connAttrValue, String setter, String setValue) 
    throws SQLException {
        JDBCDataSource.setBeanProperty(ds, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(ds, setter, setValue);
        JDBCDataSource.setBeanProperty(
            ds, "ConnectionAttributes", connAttrValue);
        // check that the db exists; execute an unnecessary, but harmless, stmt
        try {
            if (ds instanceof javax.sql.ConnectionPoolDataSource)
                ((ConnectionPoolDataSource)ds).getPooledConnection();
            else
                ((XADataSource)ds).getXAConnection();
            fail("expected an sqlexception " + expectedSQLState);
        } catch (SQLException se) {
            assertSQLState(expectedSQLState, se);
        }
        JDBCDataSource.clearStringBeanProperty(ds, setter);
        JDBCDataSource.clearStringBeanProperty(ds, "ConnectionAttributes");
    }

    // sets ConnectionAttributes first, then SetCreate/ShutdownDB
    protected void assertSetConOK(Object ds, String expectedSQLState, 
        String dbName, String connAttrValue, String setter, String setValue) 
    throws SQLException {
        JDBCDataSource.setBeanProperty(ds, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(
            ds, "ConnectionAttributes", connAttrValue);
        JDBCDataSource.setBeanProperty(ds, setter, setValue);
        // check that the db exists; execute an unnecessary, but harmless, stmt
        try {
            
            if (ds instanceof javax.sql.ConnectionPoolDataSource)
                ((ConnectionPoolDataSource)ds).getPooledConnection();
            else
                ((XADataSource)ds).getXAConnection();
            fail("expected an sqlexception " + expectedSQLState);
        } catch (SQLException se) {
            assertSQLState(expectedSQLState, se);
        }
        JDBCDataSource.clearStringBeanProperty(ds, "ConnectionAttributes");
        JDBCDataSource.clearStringBeanProperty(ds, setter);
    }

    protected void assertUpdateCount(Object ds) throws SQLException {        
        if (ds instanceof javax.sql.ConnectionPoolDataSource)
            assertUpdateCount(
                ((ConnectionPoolDataSource)ds).getPooledConnection().getConnection().createStatement(), 0, "set schema SPLICE");
        else
            assertUpdateCount(
                ((XADataSource)ds).getXAConnection().getConnection().createStatement(), 0, "set schema SPLICE");
    }
    
    
    /**
     * Clear bean properties for next test
     * @param ds
     * @throws SQLException
     */
    private void clearBeanProperties(Object ds) throws SQLException {
        JDBCDataSource.clearStringBeanProperty(ds, "createDatabase");
        JDBCDataSource.clearStringBeanProperty(ds, "shutdownDatabase");
        JDBCDataSource.clearStringBeanProperty(ds, "connectionAttributes");
        JDBCDataSource.clearStringBeanProperty(ds, "databaseName");
    }
 
}
