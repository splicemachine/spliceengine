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

package com.splicemachine.dbTesting.functionTests.tests.jdbcapi;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.JDBCDataSource;
import com.splicemachine.dbTesting.junit.SecurityManagerSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;


public class InvalidLDAPServerAuthenticationTest extends BaseJDBCTestCase {

    /** Creates a new instance of the Test */
    public InvalidLDAPServerAuthenticationTest(String name) {
        super(name);
    }

    /**
     * Ensure all connections are not in auto commit mode.
     */
    protected void initializeConnection(Connection conn) throws SQLException {
        conn.setAutoCommit(false);
    }

    public static Test suite() {
        if (JDBC.vmSupportsJSR169())
            return new TestSuite("InvalidLDAPServerAuthenticationTest - cannot" +
                " run with JSR169 - missing functionality for " +
                "com.splicemachine.db.iapi.jdbc.AuthenticationService");
        
        // security manager would choke attempting to resolve to the invalid
        // LDAPServer, so run without
        TestSuite suite = new TestSuite("InvalidLDAPServerAuthenticationTest");
        suite.addTest(SecurityManagerSetup.noSecurityManager(baseSuite(
                "testInvalidLDAPServerConnectionError")));
        suite.addTest(TestConfiguration.clientServerDecorator(
                SecurityManagerSetup.noSecurityManager(
                baseSuite("testInvalidLDAPServerConnectionError"))));
        return suite;            
    }

    public static Test baseSuite(String name) {
        TestSuite suite = new TestSuite(name);
        Test test = new InvalidLDAPServerAuthenticationTest("testInvalidLDAPServerConnectionError");
        suite.addTest(test);

        // This test needs to run in a new single use database without connect
        // for shutdown after, as we're going to make the database unusable
        return TestConfiguration.singleUseDatabaseDecoratorNoShutdown(suite);
    }

    protected void setDatabaseProperty(
            String propertyName, String value, Connection conn) 
    throws SQLException {
        CallableStatement setDBP =  conn.prepareCall(
        "CALL SYSCS_UTIL.SYSCS_SET_DATABASE_PROPERTY(?, ?)");
        setDBP.setString(1, propertyName);
        setDBP.setString(2, value);
        setDBP.execute();
        setDBP.close();
    }

    public void testInvalidLDAPServerConnectionError() throws SQLException {
        // setup 
        Connection conn = getConnection();
        // set the ldap properties
        setDatabaseProperty("derby.connection.requireAuthentication", "true", conn);
        setDatabaseProperty("derby.authentication.provider", "LDAP", conn);
        setDatabaseProperty("derby.authentication.server", "noSuchServer", conn);
        setDatabaseProperty("derby.authentication.ldap.searchBase", "o=dnString", conn);
        setDatabaseProperty("derby.authentication.ldap.searchFilter","(&(objectClass=inetOrgPerson)(uid=%USERNAME%))", conn);
        commit();
        conn.setAutoCommit(true);
        conn.close();
        // shutdown the database as system, so the properties take effect
        TestConfiguration.getCurrent().shutdownDatabase();
        String dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();
        
        // actual test. 
        // first, try datasource connection
        DataSource ds = JDBCDataSource.getDataSource(dbName);
        try {
            ds.getConnection();
            fail("expected java.net.UnknownHostException for datasource");
        } catch (SQLException se) {
            assertSQLState("08004", se);
            // with network server, the java.net.UnknownHostException will be in 
            // db.log, the client only gets a 08004 and somewhat misleading
            // warning ('Reason: userid or password invalid')
            if (usingEmbedded())
                assertTrue(se.getMessage().indexOf("java.net.UnknownHostException")>1);
        }
        // driver manager connection
        String url2 = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        try {
            DriverManager.getConnection(url2,"user","password").close();
            fail("expected java.net.UnknownHostException for driver");
        } catch (SQLException se) {
            assertSQLState("08004", se);
            // with network server, the java.net.UnknownHostException will be in 
            // db.log, the client only gets a 08004 and somewhat misleading
            // warning ('Reason: userid or password invalid')
            if (usingEmbedded())
                assertTrue(se.getMessage().indexOf("java.net.UnknownHostException")>1);
        }
        
        // we need to shutdown the system, or the failed connections
        // cling to db.lck causing cleanup to fail.
        // we *can* shutdown because we don't have authentication required
        // set at system level (only database level).
        shutdownSystem();
    }
    
    protected void shutdownSystem()throws SQLException {
        DataSource ds;
        if (usingEmbedded())
        {
            ds = JDBCDataSource.getDataSource();
            JDBCDataSource.clearStringBeanProperty(ds, "databaseName");
        }
        else
        {
            // note: with network server/client, you can't set the databaseName
            // to null, that results in error 08001 - Required DataSource
            // property databaseName not set.
            // so, we rely on passing of an empty string for databaseName,
            // which in the current code is interpreted as system shutdown.
            ds = JDBCDataSource.getDataSource("");
        }
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        try {
            ds.getConnection();
        } catch (SQLException e) {
            //do nothing;
        }
    }
}