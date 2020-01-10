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

import java.sql.SQLException;

import javax.sql.XADataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.J2EEDataSource;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.JDBCDataSource;
import com.splicemachine.dbTesting.junit.TestConfiguration;

//Extends AuthenticationTest.java which only holds XADataSource calls.
//This class implements the checks for XADataSources
public class XADSAuthenticationTest extends AuthenticationTest {
    
    /** Creates a new instance of the Test */
    public XADSAuthenticationTest(String name) {
        super(name);
    }

    public static Test suite() {
        // This test uses XADataSource and so is not suitable for JSR169
        if (JDBC.vmSupportsJSR169())
            return new TestSuite("");
        else {
            TestSuite suite = new TestSuite("XADSAuthenticationTest");
            suite.addTest(baseSuite("XADSAuthenticationTest:embedded"));
            suite.addTest(TestConfiguration.clientServerDecorator(
                baseSuite("XADSAuthenticationTest:client")));
            return suite;
        }
    }
    
    // baseSuite takes advantage of setting system properties as defined
    // in AuthenticationTest
    public static Test baseSuite(String name) {
        TestSuite suite = new TestSuite("XADSAuthenticationTest");

        Test test = new XADSAuthenticationTest(
            "testConnectShutdownAuthentication");
        setBaseProps(suite, test);
        
        test = new XADSAuthenticationTest("testUserFunctions");
        setBaseProps(suite, test);

        test = new XADSAuthenticationTest("testNotFullAccessUsers");
        setBaseProps(suite, test);
        
        test = new XADSAuthenticationTest(
            "testChangePasswordAndDatabasePropertiesOnly");
        setBaseProps(suite, test);

        // only part of this fixture runs with network server / client
        test = new XADSAuthenticationTest("testGreekCharacters");
        setBaseProps(suite, test);
        
        test = new XADSAuthenticationTest("testSystemShutdown");
        setBaseProps(suite, test);

        // The test needs to run in a new single use database as we're setting
        // a number of properties
        return TestConfiguration.singleUseDatabaseDecorator(suite);
    }
    
    protected void assertConnectionOK(
        String dbName, String user, String password)
    throws SQLException 
    {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        xads.getXAConnection(user, password).close();

    }

    protected void assertConnectionWOUPOK(
            String dbName, String user, String password)
    throws SQLException
    {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(xads, "user", user);
        JDBCDataSource.setBeanProperty(xads, "password", password);
        xads.getXAConnection().close();
    }
    
    protected void assertConnectionFail(
        String expectedSqlState, String dbName, String user, String password)
    throws SQLException
    {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        try {
            xads.getXAConnection(user, password);
            fail("Connection should've been refused/failed");
        } catch (SQLException sqle) {
            assertSQLState(expectedSqlState, sqle);
        }
    }

    protected void assertConnectionWOUPFail(
        String expectedSqlState, String dbName, String user, String password)
    throws SQLException
    {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(xads, "user", user);
        JDBCDataSource.setBeanProperty(xads, "password", password);
        try {
            xads.getXAConnection();
            fail("Connection should've been refused/failed");
        }
        catch (SQLException e) {
            assertSQLState(expectedSqlState, e);
        }
    }
    
    protected void assertShutdownUsingSetShutdownOK(
            String dbName, String user, String password) throws SQLException {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(
            xads, "shutdownDatabase", "shutdown");
        try {
            xads.getXAConnection(user, password);
            fail ("expected a failed shutdown connection");
        } catch (SQLException e) {
            // expect 08006 on successful shutdown
            assertSQLState("08006", e);
        }
    }

    protected void assertShutdownUsingConnAttrsOK(
        String dbName, String user, String password) throws SQLException {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(
            xads, "connectionAttributes", "shutdown=true");
        try {
            xads.getXAConnection(user, password);
            fail("expected shutdown to fail");
        } catch (SQLException e) {
            // expect 08006 on successful shutdown
            assertSQLState("08006", e);
        }
    }

    protected void assertShutdownWOUPOK(
        String dbName, String user, String password)
    throws SQLException {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(
                xads, "shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(xads, "user", user);
        JDBCDataSource.setBeanProperty(xads, "password", password);
        try {
            xads.getXAConnection();
            fail ("expected a failed shutdown connection");
        } catch (SQLException e) {
            // expect 08006 on successful shutdown
            assertSQLState("08006", e);
        }
    }

    protected void assertShutdownFail(
        String expectedSqlState, String dbName, String user, String password) 
    throws SQLException
    {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        try {
            xads.getXAConnection(user, password);
            fail("expected failed shutdown");
        } catch (SQLException e) {
            assertSQLState(expectedSqlState, e);
        }
    }
            
    protected void assertShutdownWOUPFail(
        String expectedSqlState, String dbName, String user, String password) 
    throws SQLException
    {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(xads, "shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(xads, "user", user);
        JDBCDataSource.setBeanProperty(xads, "password", password);
        try {
            xads.getXAConnection();
            fail("expected failed shutdown");
        } catch (SQLException e) {
            assertSQLState(expectedSqlState, e);
        }
    }

    protected void assertSystemShutdownOK(
        String dbName, String user, String password)
    throws SQLException {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(
                xads, "shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(xads, "user", user);
        JDBCDataSource.setBeanProperty(xads, "password", password);
        try {
            xads.getXAConnection();
            fail("expected system shutdown resulting in XJ015 error");
        } catch (SQLException e) {
            // expect XJ015, system shutdown, on successful shutdown
            assertSQLState("XJ015", e);
        }
    }

    protected void assertSystemShutdownFail(
        String expectedError, String dbName, String user, String password)
    throws SQLException {
        XADataSource xads = J2EEDataSource.getXADataSource();
        JDBCDataSource.setBeanProperty(
                xads, "shutdownDatabase", "shutdown");
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        JDBCDataSource.setBeanProperty(xads, "user", user);
        JDBCDataSource.setBeanProperty(xads, "password", password);
        try {
            xads.getXAConnection();
            fail("expected shutdown to fail");
        } catch (SQLException e) {
            assertSQLState(expectedError, e);
        }
    }

    public void assertConnectionFail(String dbName) throws SQLException {
        XADataSource xads = J2EEDataSource.getXADataSource();
        // Reset to no user/password though client requires
        // a valid name, so reset to the default
        if (usingDerbyNetClient())
            JDBCDataSource.setBeanProperty(xads, "user", "SPLICE");
        else
            JDBCDataSource.clearStringBeanProperty(xads, "user");
        JDBCDataSource.clearStringBeanProperty(xads, "password");
        JDBCDataSource.setBeanProperty(xads, "databaseName", dbName);
        try {
            xads.getXAConnection();
            fail("expected connection to fail");
        } catch (SQLException e) {
            assertSQLState("08004", e);
        }
    }
}
