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
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.PooledConnection;
import javax.sql.XAConnection;
import javax.sql.XADataSource;

import junit.framework.Test;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.J2EEDataSource;
import com.splicemachine.dbTesting.junit.TestConfiguration;

public class InternationalConnectTest extends BaseJDBCTestCase {

    /**
     * Test connecting with multibyte characters in:
     * - Database name
     * - User
     * - Password
     * 
     * Currently just throws an exception for client.
     * Works ok for embedded.
     * 
     * This test tests DriverManager, XADataSource and ConnectionPoolDataSource
     * and is not run with J2ME.  Simple DataSource is tested with 
     * InternationalConnectSimpleDSTest
     * 
     */
   
    /* Keep track of the databases created in the fixtures to cleanup in tearDown() */
    private ArrayList databasesForCleanup;
    
    /**
     * @param name
     */
    public InternationalConnectTest(String name) {
        super(name);
        
        databasesForCleanup = new ArrayList();
    }

    public void testBoundaries() throws SQLException, UnsupportedEncodingException {
        if (usingEmbedded()) return; /* This test is only for Client/Server */

        // ensuring that we get a connection.
        getConnection();
        
        /*
         * Sun's 1.4.2 JVM and IBM's JVM (any version) fail on Windows for this test
         * Thus, we skip it.
         * 
         * Read JIRA's DERBY-4836 for more information.
         */
        if (getSystemProperty("os.name").startsWith("Windows")) {            
            /* Skip with 1.4.2 jvms */
            if (getSystemProperty("java.version").startsWith("1.4.2")) return;
        }
        
        // Maximum length in bytes is 255. We subtract 14 to account for
        // ;create=true and ;shutdown=true
        int maxNameLength = 255 - 14;
        
        /**
         * \u0041 is the letter 'A' (1 byte)
         * \u00e7 is the letter 'c' with a cedilla (2 bytes)
         * \u4310 is a Chinese character (3 bytes)
         * \u1f030 is a domino tile (4 bytes)
         */
        String[] testCharacters = {"\u0041", "\u00e7", "\u4e10", "\u1f030"}; 
        
        for (int ch=0; ch<testCharacters.length; ch++) {
            StringBuffer dbName = new StringBuffer();
            
            /* max length in bytes divided by length of 1 chinese char */ 
            int maxChars = maxNameLength / testCharacters[ch].getBytes("UTF-8").length;
            for(int i=0; i<maxChars; i++) {
                dbName.append(testCharacters[ch]);
            }
            
            /* This time it should work as we're right at the limit */
            String url = TestConfiguration
                    .getCurrent().getJDBCUrl(dbName.toString()+ ";create=true");
            
            Connection conn = DriverManager.getConnection(url);
            conn.close();
            
            /* Add the database name for cleanup on tearDown() */
            databasesForCleanup.add(dbName.toString());
            
            /* Append three more characters to make it fail */
            for (int i = 0; i < 3; i++) {
                dbName.append(testCharacters[ch]);
            }

            url = TestConfiguration
                    .getCurrent().getJDBCUrl(dbName.toString()+ ";create=true");

            try {
                conn = DriverManager.getConnection(url);
                assertTrue("Used more characters than possible in database name",
                        false);
            } catch (SQLException e) {
                assertSQLState("08001", e); /* Check if it failed */
            }
        }
    }
    
    /**
     * Test Chinese character in database name, user and password, using 
     * DriverManager methods.
     * 
     * @throws SQLException
     */
    public void testDriverManagerConnect() throws SQLException {        
        //get a connection to load the driver
        getConnection();
        Connection conn = null;
        String url = null;

        //Test Chinese database name
        url = TestConfiguration.getCurrent().getJDBCUrl("\u4e10;create=true");
        
        conn = DriverManager.getConnection(url);
        conn.close();           

        // Test Chinese user name
        url = TestConfiguration.getCurrent().getJDBCUrl("\u4e10;user=\u4e10");
        conn = DriverManager.getConnection(url);
        conn.close();

        // Test Chinese user name in parameter to getConnection
        url = TestConfiguration.getCurrent().getJDBCUrl("\u4e10");
        conn = DriverManager.getConnection(url,"\u4e10","pass");
        conn.close();

        // Test Chinese password in url
        url = TestConfiguration.getCurrent().getJDBCUrl("\u4e10;user=user;password=\u4e10");
        conn = DriverManager.getConnection(url);
        conn.close();

        // Test Chinese password in parameter to getConnection()
        url = TestConfiguration.getCurrent().getJDBCUrl("\u4e10");
        conn = DriverManager.getConnection(url,"\u4e10","\u4e10");
        conn.close();
        
        /* Add the created database for cleanup by tearDown() */
        databasesForCleanup.add("\u4e10");
    }
    
    
    /**
     * Test XA Connection for chinese database name, user and password.
     * @throws SQLException
     */
    public void testXADSConnect() throws SQLException {        
        // Test chinese database name.
        XADataSource ds = J2EEDataSource.getXADataSource();
        J2EEDataSource.setBeanProperty(ds, "databaseName", "\u4e10");
        J2EEDataSource.setBeanProperty(ds, "createDatabase", "create");        

        XAConnection xaconn = ds.getXAConnection();
        Connection conn = xaconn.getConnection();
        conn.close();
        xaconn.close();
  
        // Chinese user
        J2EEDataSource.setBeanProperty(ds, "user", "\u4e10");
        xaconn = ds.getXAConnection();
        conn = xaconn.getConnection();
        conn.close();
        xaconn.close();

        // Chinese password
        J2EEDataSource.setBeanProperty(ds, "password", "\u4e10");
        xaconn = ds.getXAConnection();
        conn = xaconn.getConnection();
        conn.close();
        xaconn.close();
        
        /* Add the created database for cleanup by tearDown() */
        databasesForCleanup.add("\u4e10");
    }
    
    
    /**
     * Test pooled connetion for chinese database name, user and password.
     * @throws SQLException
     */
    public void testCPDSConnect() throws SQLException {
        // Test chinese database name.
        ConnectionPoolDataSource ds = J2EEDataSource.getConnectionPoolDataSource();
        J2EEDataSource.setBeanProperty(ds, "databaseName", "\u4e10");
        J2EEDataSource.setBeanProperty(ds, "createDatabase", "create");        

        PooledConnection poolConn = ds.getPooledConnection();
        Connection conn = poolConn.getConnection();
        conn.close();
        poolConn.close();
 
        // Chinese user
        J2EEDataSource.setBeanProperty(ds, "user", "\u4e10");
        poolConn = ds.getPooledConnection();
        conn = poolConn.getConnection();
        conn.close();
        poolConn.close();

        // Chinese password
        J2EEDataSource.setBeanProperty(ds, "password", "\u4e10");
        poolConn= ds.getPooledConnection();
        conn = poolConn.getConnection();
        conn.close();
        poolConn.close();
        
        /* Add the created database for cleanup by tearDown() */
        databasesForCleanup.add("\u4e10");
    }

    /**
     * Regression test case for DERBY-4799. Attempting to connect to a
     * database that doesn't exist used to cause a protocol error between
     * the network server and the client. This only happened if the
     * database name was at least 18 characters and the name contained at
     * least one non-ascii character.
     */
    public void testFailureOnNonExistentDatabase() throws SQLException {
        
        String url = TestConfiguration.getCurrent().getJDBCUrl(
                "abcdefghijklmnopq\u00E5");
        try {
            // This call used to fail with a protocol error with the
            // client driver. Check that it fails gracefully now.
            DriverManager.getConnection(url);
            fail(url + " should not exist");
        } catch (SQLException sqle) {
            // Embedded responds with XJ004 - database not found.
            // Client responds with 08004 - connection refused because
            // the database was not found.
            String expected = usingEmbedded() ? "XJ004" : "08004";
            assertSQLState(expected, sqle);
        }
    }

    public void tearDown() throws Exception {
        /* Iterate through the databases for cleanup and delete them */
        for (int i=0; i<databasesForCleanup.size(); i++) {
            String shutdownUrl = TestConfiguration.getCurrent()
                                .getJDBCUrl(databasesForCleanup.get(i) + ";shutdown=true");
            try {
                DriverManager.getConnection(shutdownUrl);
                fail("Database didn't shut down");
            } catch (SQLException se) {
                // ignore shutdown exception
                assertSQLState("08006", se);
            }
            removeDirectory(getSystemProperty("derby.system.home") +  File.separator +
                    databasesForCleanup.get(i));
        }
        
        /* Clear the array list as new fixtures will add other databases */
        databasesForCleanup = null;

        super.tearDown();
    }
    
    public static Test suite() {        
        return TestConfiguration.defaultSuite(InternationalConnectTest.class);
    }
   
}
