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

import java.io.File;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import javax.sql.DataSource;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.DatabasePropertyTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.JDBCDataSource;
import com.splicemachine.dbTesting.junit.SecurityManagerSetup;
import com.splicemachine.dbTesting.junit.SupportFilesSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;

// tests that appropriate and invalid connections can be made to 
// an LDAPServer when property authenticationProvider is set to 'LDAP'.
// This test assumes at least one valid user (to be passed in) and one
// additional user (kathy / kathyS) to be setup in ou=People on the
// LDAPServer.
public class LDAPAuthenticationTest extends BaseJDBCTestCase {

    private static String ldapServer;
    private static String ldapPort;
    private static String dnString;
    private static String ldapUser; // existing valid user on ldap server
    private static String ldapPassword; // password for existing valid user on ldap server
    private static String ldapContextFactory; // optional initial context factory
        // if not passed in with -DderbyTesting.ldapContextFactory, uses sun's

    // create own policy file, so we can connect to the ldap server
    private static String POLICY_FILE_NAME = 
        "functionTests/tests/jdbcapi/LDAPTests.policy";
    private static String TARGET_POLICY_FILE_NAME = "derby_tests.policy";
    
    /** Creates a new instance of the Test */
    public LDAPAuthenticationTest(String name) {
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
            return new TestSuite("cannt run with JSR169 - missing functionality" +
                " for com.splicemachine.db.iapi.jdbc.AuthenticationService");
        ldapUser=getSystemProperty("derbyTesting.ldapUser");
        if (ldapUser == null || ldapUser.length() < 1)
            return new TestSuite("LDAPAuthenticationTest requires property " +
                "derbyTesting.ldapUser set to a valid user set up on the " +
                "ldapServer, eg: -DderbyTesting.ldapUser=CharliesPwd. In addition," +
                "test requires a user 'kathy', pwd 'kathyS' to be set up");
        ldapPassword=getSystemProperty("derbyTesting.ldapPassword");
        if (ldapPassword == null || ldapPassword.length() < 1)
            return new TestSuite("LDAPAuthenticationTest requires property " +
                "derbyTesting.ldapPassword set the password of a valid user set" +
                " up on the ldapServer, eg: -DderbyTesting.ldapPassword=Charlie");
        ldapServer=getSystemProperty("derbyTesting.ldapServer");
        if (ldapServer == null || ldapServer.length() < 1)
            return new TestSuite("LDAPAuthenticationTest requires property " +
                "derbyTesting.ldapServer set, eg: " +
                "-DderbyTesting.ldapServer=myldapserver.myorg.org");
        ldapPort=getSystemProperty("derbyTesting.ldapPort");
        if (ldapPort == null || ldapPort.length() < 1)
            return new TestSuite("LDAPAuthenticationTest requires property " +
                "derbyTesting.ldapPort set, eg: -DderbyTesting.ldapPort=333");
        dnString=getSystemProperty("derbyTesting.dnString");
        if (dnString == null || dnString.length() < 1)
            return new TestSuite("LDAPAuthenticationTest requires property " +
                "derbyTesting.dnString for setting o=, eg: " +
                "-DderbyTesting.dnString=myJNDIstring");
        ldapContextFactory=getSystemProperty("derbyTesting.ldapContextFactory");

        TestSuite suite = new TestSuite("LDAPAuthenticationTest");
        suite.addTest(baseSuite("LDAPAuthenticationTest:embedded",
            "testLDAPConnection"));
        suite.addTest(TestConfiguration.clientServerDecorator(
            baseSuite("LDAPAuthenticationTest:client", "testLDAPConnection")));
        Test test = decorateWithPolicy(suite);
        return test;            
    }

    public static Test baseSuite(String name, String fixture) {
        TestSuite suite = new TestSuite(name);
        Test test = new LDAPAuthenticationTest(fixture);
        setBaseProps(suite, test);

        // This test needs to run in a new single use database as we're setting
        // a number of properties
        return TestConfiguration.singleUseDatabaseDecorator(suite);
    }

    protected static void setBaseProps(TestSuite suite, Test test) 
    {
        // set some debugging at database level properties
        Properties props = new Properties();
        props.setProperty("derby.infolog.append", "true");
        props.setProperty("derby.debug.true", "AuthenticationTrace");
        // add some users. these should not be defined on the ldap server
        props.setProperty("derby.user.system", "manager");
        props.setProperty("derby.user.Jamie", "theHooligan");
        suite.addTest(new DatabasePropertyTestSetup (test, props, true));
    }

    // grant ALL FILES execute, and getPolicy permissions, as well
    // as resolve/connect for the ldap server identified with the property
    private static Test decorateWithPolicy(Test test) {
        String ldapPolicyName = new LDAPAuthenticationTest("test").makeServerPolicyName();
        //
        // Install a security manager using the initial policy file.
        //
        test = new SecurityManagerSetup(test,ldapPolicyName );
        // Copy over the policy file we want to use.
        //
        test = new SupportFilesSetup(
            test, null, new String[] {POLICY_FILE_NAME},
            null, new String[] {TARGET_POLICY_FILE_NAME}
        );
        return test;
    }
    
    /**
     * Construct the name of the server policy file.
     */
    private String makeServerPolicyName()
    {
        try {
            String  userDir = getSystemProperty( "user.dir" );
            String  fileName = userDir + File.separator + SupportFilesSetup.EXTINOUT + File.separator + TARGET_POLICY_FILE_NAME;
            File      file = new File( fileName );
            String  urlString = file.toURL().toExternalForm();

            return urlString;
        }
        catch (Exception e)
        {
            return null;
        }
    }
    
    protected void tearDown() throws Exception {
        removeSystemProperty("derby.connection.requireAuthentication");
        super.tearDown();
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

    protected String getDatabaseProperty(
            String propertyName, Connection conn) 
    throws SQLException {
        PreparedStatement getDBP =  conn.prepareStatement(
        "VALUES SYSCS_UTIL.SYSCS_GET_DATABASE_PROPERTY(?)");
        getDBP.setString(1, propertyName);
        ResultSet rs = getDBP.executeQuery();
        rs.next();
        String value = rs.getString(1);
        rs.close();
        return value;
    }    
     
    public void testLDAPConnection() throws SQLException {
        // setup 
        String dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();
        DataSource ds = JDBCDataSource.getDataSource();
        Connection conn = ds.getConnection("system", "admin");
        // set the ldap properties
        setDatabaseProperty("derby.connection.requireAuthentication", "true", conn);
        setDatabaseProperty("derby.authentication.provider", "LDAP", conn);
        setDatabaseProperty("derby.authentication.server", ldapServer, conn);
        setDatabaseProperty("derby.authentication.ldap.searchBase", "o=" + dnString, conn);
        setDatabaseProperty("derby.authentication.ldap.searchFilter","(&(objectClass=inetOrgPerson)(uid=%USERNAME%))", conn);
        // java.naming.factory.initial is Context.INITIAL_CONTEXT_FACTORY
        // but using literal string here to avoid unnecessary import.
        // If the initial context factory is not provided it'll default to 
        // com.sun.jndi.ldap.LdapCtxFactory in LDAPAuthenticationSchemeImpl.
        if ((ldapContextFactory != null) && (ldapContextFactory.length() > 0))
            setDatabaseProperty("java.naming.factory.initial", ldapContextFactory, conn);
        commit();
        // shutdown the database as system, so the properties take effect
        TestConfiguration.getCurrent().shutdownDatabase();
        conn.close();

        // actual test. 
        // first attempt simple connection that should succeed
        ds = JDBCDataSource.getDataSource(dbName);
        assertLDAPDSConnectionOK(ds, ldapUser, ldapPassword);
        assertLDAPDrvMgrConnectionOK(dbName, ldapUser, ldapPassword);
        // try to get a connection for a user that has been added, but who
        // should *not* be on the ldap server; should fail
        assertInvalidLDAPDSConnectionFails(ds, "Jamie", "theHooligan");
        assertInvalidLDAPDrvMgrConnectionFails(dbName, "Jamie", "thHooligan");
        // try to get a connection using the valid ldapuser, but incorrect pwd
        assertInvalidLDAPDSConnectionFails(ds, ldapUser, ldapPassword + "ish");
        assertInvalidLDAPDrvMgrConnectionFails(dbName, ldapUser, ldapPassword + "ish");

        // set the users DN locally
        Connection conn3 = ds.getConnection(ldapUser, ldapPassword);
        String tmpString1 = "derby.user." + ldapUser;
        String tmpString2 = "uid=" + ldapUser + ",ou=People,o=" + dnString; 
        setDatabaseProperty(tmpString1, tmpString2, conn3);
        conn3.commit();
        // restart to let setting take effect
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        // shutdown really only happens on next attempt to connect
        try {
            ds.getConnection(ldapUser, ldapPassword);
            fail("expected system shutdown resulting in 08006 error.");
        } catch (SQLException e) {
            assertSQLState("08006", e);
        }
        conn3.close();
        
        // It is now required to re-establish the name
        // of the database...
        dbName = TestConfiguration.getCurrent().getDefaultDatabaseName();
        ds = JDBCDataSource.getDataSource(dbName);
        // test again
        // connect using a valid ldap user/pwd
        assertLDAPDSConnectionOK(ds, ldapUser, ldapPassword);
        assertLDAPDrvMgrConnectionOK(dbName, ldapUser, ldapPassword);
        // try to use a not specified elsewhere, but hopefully valid ldapUser:
        // as no local DN is cached, look-up will be performed with the default
        // search filter.
        assertLDAPDSConnectionOK(ds, "kathy", "kathyS");
        assertLDAPDrvMgrConnectionOK(dbName, "kathy", "kathyS");
        // again try to get a connection for a user that has been added, but
        // should *not* be on the ldap server; should fail
        assertInvalidLDAPDSConnectionFails(ds, "Jamie", "theHooligan");
        assertInvalidLDAPDrvMgrConnectionFails(dbName, "Jamie", "theHooligan");
        // try to get a connection using the valid ldapuser, but incorrect pwd
        assertInvalidLDAPDSConnectionFails(ds, ldapUser, ldapPassword + "ish");
        assertInvalidLDAPDrvMgrConnectionFails(dbName, ldapUser, ldapPassword + "ish");
        
        assertLDAPDSConnectionOK(ds, ldapUser, ldapPassword);
        // as we don't have requireAuthentication, or the authentication provider,
        // set on system level, we can shutdown the system with any user.
        assertDSSystemShutdownOK("someuser", "somestring");
        
        // cleanup. Can't use default teardown mechanism yet as it uses default
        // user/password and we're set to LDAP Authentication at this point.
        cleanup(ds);
    }

    // cleanup gets a conn using the ldap user & pwd, so we can unset the
    // requireAuthentication so the default teardown & db cleanup can happen
    protected void cleanup(DataSource ds) throws SQLException {
        Connection conn2 = ds.getConnection(ldapUser, ldapPassword);
        setDatabaseProperty("derby.connection.requireAuthentication", "false", conn2);
        conn2.commit();
        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        // shutdown only really happens on attempting to get a connection;
        // this exercise is needed so the DatabasePropertySetup.teardown() can work.
        // Getting a 08006 is not really part of the test, but would be good
        // to be warned of if it happens.
        try {
            ds.getConnection(ldapUser, ldapPassword);
            fail("expected system shutdown resulting in 08006 error.");
        } catch (SQLException e) {
            assertSQLState("08006", e);
        }
        conn2.close();
    }

    public void assertLDAPDSConnectionOK(
            DataSource ds, String user, String password)
    throws SQLException {
        Connection conn = ds.getConnection(user, password);
        assertNotNull(conn);
        conn.close();
    }

    protected void assertLDAPDrvMgrConnectionOK(
            String dbName, String user, String password)
    throws SQLException
    {
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        Connection conn = DriverManager.getConnection(url, user, password);
        assertNotNull(conn);
        conn.close();
    }

    public void assertInvalidLDAPDSConnectionFails(
            DataSource ds, String user, String password)
    throws SQLException {
        try {
            ds.getConnection(user, password);
            fail("expected invalid connection error (for DataSource)");
        } catch (SQLException se) {
            assertSQLState("08004", se);
        }
    }

    protected void assertInvalidLDAPDrvMgrConnectionFails(
            String dbName, String user, String password)
    throws SQLException
    {
        String url = TestConfiguration.getCurrent().getJDBCUrl(dbName);
        try {
            DriverManager.getConnection(url, user, password).close();
            fail("expected invalid connection error (for DataSource)");
        } catch (SQLException se) {
            assertSQLState("08004", se);
        }
    }

    protected void assertDSSystemShutdownOK(
            String user, String password)
    throws SQLException {
        DataSource ds;
        if (usingEmbedded())
        {
            // we cannot use JDBCDataSource.getDataSource() (which uses the
            // default database name), unless we specifically clear the 
            // databaseName. Otherwise, only the database will be shutdown.
            // The alternative is to use jDBCDataSource.getDatasource(dbName),
            // where dbName is an empty string - this will in the current code
            // be interpreted as a system shutdown.
            ds = JDBCDataSource.getDataSource();
            JDBCDataSource.clearStringBeanProperty(ds, "databaseName");
        }
        else 
        {
            // With client, we cannot user clearStringBeanProperty on the  
            // databaseName, that will result in error 08001 - 
            // Required DataSource property databaseName not set.
            // So, we pass an empty string as databaseName, which the current
            // code interprets as a system shutdown.
            ds = JDBCDataSource.getDataSource("");
        }

        JDBCDataSource.setBeanProperty(ds, "shutdownDatabase", "shutdown");
        try {
            ds.getConnection(user, password);
            fail("expected system shutdown resulting in XJ015 error");
        } catch (SQLException e) {
            // expect XJ015, system shutdown, on successful shutdown
            assertSQLState("XJ015", e);
        }
    }
}