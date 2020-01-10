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

import java.sql.DriverManager;
import java.sql.SQLException;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;


// Extends AuthenticationTest.java which only holds DataSource calls
// this class uses some of the same methods but uses DriverManager to
// obtain connections
public class DriverMgrAuthenticationTest extends AuthenticationTest {

    /** Creates a new instance of the Test */
    public DriverMgrAuthenticationTest(String name) {
        super(name);
    }

    public static Test suite() {
        // This test uses driverManager and so is not suitable for JSR169
        if (JDBC.vmSupportsJSR169())
            return new TestSuite("DriverManager not available with JSR169;" +
                "empty DriverMgrAuthenticationTest");
        else {
            TestSuite suite = new TestSuite("DriverMgrAuthenticationTest");
            suite.addTest(
                baseSuite("DriverMgrAuthenticationTest:embedded"));
            suite.addTest(TestConfiguration.clientServerDecorator(
                baseSuite("DriverMgrAuthenticationTest:client")));
            return suite;
        }
    }

    // baseSuite takes advantage of setting system properties as defined
    // in AuthenticationTest
    public static Test baseSuite(String name) {
        TestSuite suite = new TestSuite("DriverMgrAuthenticationTest");
        
        Test test = new DriverMgrAuthenticationTest(
            "testConnectShutdownAuthentication");
        setBaseProps(suite, test);
        
        test = new DriverMgrAuthenticationTest("testUserFunctions");
        setBaseProps(suite, test);

        test = new DriverMgrAuthenticationTest("testNotFullAccessUsers");
        setBaseProps(suite, test);
        
        test = new DriverMgrAuthenticationTest(
            "testChangePasswordAndDatabasePropertiesOnly");
        setBaseProps(suite, test);

        // only part of this fixture runs with network server / client
        test = new DriverMgrAuthenticationTest("testGreekCharacters");
        setBaseProps(suite, test);

        test = new DriverMgrAuthenticationTest("testSystemShutdown");
        setBaseProps(suite, test);
        
        // The test needs to run in a new single use database as we're
        // setting a number of properties
        return TestConfiguration.singleUseDatabaseDecorator(suite);
    }
    
    protected void assertConnectionOK(
        String dbName, String user, String password)
    throws SQLException
    {
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        DriverManager.getConnection(url, user, password).close();
    }

    // getConnection(), using url connection attributes
    protected void assertConnectionWOUPOK(
        String dbName, String user, String password)
    throws SQLException
    {
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        String url2 = url + ";user=" + user + ";password=" + password;
        DriverManager.getConnection(url2).close();
    }

    protected void assertConnectionFail(
        String expectedSqlState, String dbName, String user, String password)
    throws SQLException
    {
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        try {
            DriverManager.getConnection(url, user, password);
            fail("Connection should've been refused/failed");
        }
        catch (SQLException e) {
                assertSQLState(expectedSqlState, e);
        }

    }

    protected void assertConnectionWOUPFail(
        String expectedSqlState, String dbName, String user, String password)
    throws SQLException
    {
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        String url2 = url + ";user=" + user + ";password=" + password;
        try {
            DriverManager.getConnection(url2);
            fail("Connection should've been refused/failed");
        }
        catch (SQLException e) {
                assertSQLState(expectedSqlState, e);
        }
    }
    
    protected void assertShutdownOK(
        String dbName, String user, String password)
    throws SQLException {

        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName) +
        ";shutdown=true";
    try {
        DriverManager.getConnection(url, user, password);
            fail ("expected a failed shutdown connection");
        } catch (SQLException e) {
            // expect 08006 on successful shutdown
            assertSQLState("08006", e);
        }
    }

    // differs from assertShutdownOK by using getConnection(url)
    protected void assertShutdownWOUPOK(
        String dbName, String user, String password)
    throws SQLException {

        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        url = url + ";shutdown=true;user=" + user + ";password=" + password;
        try {
            DriverManager.getConnection(url, null);
            fail ("expected a error after shutdown connection");
        } catch (SQLException e) {
            // expect 08006 on successful shutdown
            assertSQLState("08006", e);
        }
    }
    
    protected void assertShutdownFail(
            String expectedSqlState, String dbName, String user, String password) 
    throws SQLException
    {
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName) +
            ";shutdown=true";      
        try {
            DriverManager.getConnection(url, user, password);
            fail("expected failed shutdown");
        } catch (SQLException e) {
            assertSQLState(expectedSqlState, e);
        }
    }

    // differs from assertShutdownFail in using getConnection(url)
    protected void assertShutdownWOUPFail(
        String expectedSqlState, String dbName, String user, String password) 
    throws SQLException 
    {
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        String url2 = 
            url + ";user=" + user + ";password=" + password + ";shutdown=true";
        try {
            DriverManager.getConnection(url2);
            fail("expected failed shutdown");
        } catch (SQLException e) {
            assertSQLState(expectedSqlState, e);
        }
    }

    protected void assertSystemShutdownOK(
        String dbName, String user, String password) 
    throws SQLException
    {
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        if (usingDerbyNetClient() && dbName=="")
            // The junit test harness that kicked off the test will hang when 
            // we attempt to shutdown the system - most likely because we're
            // shutting down the system while the network server thread is
            // still alive, so it gets confused...
            return;
        String url2 = 
            url + ";user=" + user + ";password=" + password + ";shutdown=true";
        try {
            DriverManager.getConnection(url2);
            fail("expected successful shutdown");
        } catch (SQLException e) {
            assertSQLState("XJ015", e);
        }
    }
    
    protected void assertSystemShutdownFail(
        String expectedSqlState, String dbName, String user, String password) 
    throws SQLException
    {
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        if (usingDerbyNetClient() && dbName=="")
            // The junit test harness that kicked off the test will hang when 
            // we attempt to shutdown the system - most likely because we're
            // shutting down the system while the network server thread is
            // still alive, so it gets confused...
            return;
        String url2 = 
            url + ";user=" + user + ";password=" + password + ";shutdown=true";
        try {
            //DriverManager.getConnection(url2, user, password);
            DriverManager.getConnection(url2);
            fail("expected failed shutdown");
        } catch (SQLException e) {
            assertSQLState(expectedSqlState, e);
        }
    }

    public void assertConnectionFail(String dbName) throws SQLException {
        // this method needs to not use default user/pwd (SPLICE, SPLICE).
        
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        try {
            DriverManager.getConnection(url);
            fail("expected connection to fail");
        }
        catch (SQLException e) {
            assertSQLState("08004", e);
        }
    }
}
