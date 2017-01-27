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

import java.sql.SQLException;
import java.sql.Connection;
import javax.sql.DataSource;
import junit.framework.Test;
import junit.framework.TestSuite;
import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.DatabasePropertyTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.JDBCDataSource;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * This JUnit tests enforcement of dbo (=database owner) powers, cf.
 * DERBY-2264.
 *
 * The tests are run in the cross product (cardinality 10) of contexts:
 *
 *    {client/server, embedded} x
 *    {no authentication, authentication and authentication/sqlAuthorization} x
 *    {data base owner, other user }
 *
 * One could consider removing the client/server suites to speed up
 * this test as it does not add much value given the nature of the changes.
 *
 */
public class DboPowersTest extends BaseJDBCTestCase
{
    /* internal state */
    final private int _authLevel;
    final private String _dbo;
    final private String _dboPassword;

    /* test execution security context: one of three below */
    final private static int NOAUTHENTICATION=0;
    final private static int AUTHENTICATION=1;
    final private static int SQLAUTHORIZATION=2;

    final private static String[] secLevelNames = {
        "noAuthentication",
        "authentication",
        "authentication + sqlAuthorization"};

    /**
     * Create a new instance of DboPowersTest (for shutdown test)
     *
     * @param name Fixture name
     * @param authLevel authentication level with which test is run
     */
    public DboPowersTest(String name, int authLevel)
    {
        super(name);
        this._authLevel = authLevel;
        this._dbo = null;
        this._dboPassword = null;
    }

    /**
     * Create a new instance of DboPowersTest (for encryption and hard
     * upgrade tests). The database owner credentials is needed to
     * always be able to perform the restricted operations (when they
     * are not under test, but used as part of a test fixture for
     * another operation).
     *
     * @param name Fixture name
     * @param authLevel authentication level with which test is run
     * @param dbo Database owner
     * @param dboPassword Database owner's password
     */

    public DboPowersTest(String name, int authLevel,
                         String dbo, String dboPassword)
    {
        super(name);
        this._authLevel = authLevel;
        this._dbo = dbo;
        this._dboPassword = dboPassword;
    }


    /**
     * Construct top level suite in this JUnit test
     *
     * @return A suite containing embedded and client suites
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite("DboPowersTest");

        /* Database shutdown powers */

        suite.addTest(dboShutdownSuite("suite: shutdown powers, embedded"));
        suite.addTest(
            TestConfiguration.clientServerDecorator(
                dboShutdownSuite("suite: shutdown powers, client")));

        /* Database (re)encryption powers
         *
         * The encryption power tests are not run for JSR169, since Derby
         * does not support database encryption for that platform, cf.
         * the specification for JSR169 support in DERBY-97.
         */
        if (!JDBC.vmSupportsJSR169()) {
            suite.addTest(
                dboEncryptionSuite("suite: encryption powers, embedded"));
            suite.addTest(
                TestConfiguration.clientServerDecorator(
                    dboEncryptionSuite("suite: encryption powers, client")));
        }

        /* Database hard upgrade powers */

        suite.addTest(
            dboHardUpgradeSuite("suite: hard upgrade powers, embedded"));
        suite.addTest(
            TestConfiguration.clientServerDecorator(
                dboHardUpgradeSuite("suite: hard upgrade powers, client")));

        return suite;
    }

    /**
     * Users used by both dboShutdownSuite and dboEncryptionSuite
     */
    final static String[][] users = {
        /* authLevel == AUTHENTICATION: dbo is SPLICE/SPLICE for db 'wombat',
         * so use that as first user.
         */
        {"SPLICE", "U1"},
        /* authLevel == SQLAUTHORIZATION: sqlAuthorizationDecorator
         * decorator presumes TEST_DBO as dbo, so add it to set of
         * valid users. Uses a fresh db 'dbsqlauth', not 'wombat'.
         */
        {"TEST_DBO", "U1"}};

    final static String pwSuffix = "pwSuffix";


    /**
     *
     * Construct suite of tests for shutdown database action
     *
     * @param framework Derby framework name
     * @return A suite containing the test case for shutdown
     * incarnated for the three security levels no authentication,
     * authentication, and authentication plus sqlAuthorization, The
     * latter two has an instance for dbo, and one for an ordinary user,
     * so there are in all five incarnations of tests.
     */
    private static Test dboShutdownSuite(String framework)
    {
        Test tests[] = new Test[SQLAUTHORIZATION+1]; // one per authLevel

        /* Tests without any authorization active (level ==
         * NOAUTHENTICATION).
         */
        TestSuite noauthSuite =
            new TestSuite("suite: security level=" +
                          secLevelNames[NOAUTHENTICATION]);
        noauthSuite.addTest(new DboPowersTest("testShutDown",
                                              NOAUTHENTICATION));
        tests[NOAUTHENTICATION] = noauthSuite;

        /* First decorate with users, then with authentication. Do this
         * twice, once for authentication only, and once for
         * authentication + sqlAuthorization (see extra decorator
         * added below).
         */
        for (int autLev = AUTHENTICATION;
             autLev <= SQLAUTHORIZATION ; autLev++) {

            tests[autLev] = wrapShutdownUserTests(autLev);
        }

        TestSuite suite = new TestSuite("dboPowers:"+framework);

        /* run tests with no authentication enabled */
        suite.addTest(tests[NOAUTHENTICATION]);

        /* run test for all users with only authentication enabled */
        suite.addTest(tests[AUTHENTICATION]);

        /* run test for all users with authentication and
         * sqlAuthorization enabled
         */
        suite.addTest(
            TestConfiguration.
            sqlAuthorizationDecorator(tests[SQLAUTHORIZATION]));

        return suite;
    }


    /**
     * Wraps the shutdown fixture in decorators to run with data
     * base owner and one other valid user.
     *
     * @param autLev security context to use
     */

    private static Test wrapShutdownUserTests(int autLev)
    {
        // add decorator for different users authenticated
        TestSuite usersSuite =
            new TestSuite("usersSuite: security level=" +
                          secLevelNames[autLev]);

        // First decorate with users, then with
        for (int userNo = 0; userNo < users.length; userNo++) {
            usersSuite.addTest
                (TestConfiguration.changeUserDecorator
                 (new DboPowersTest("testShutDown", autLev),
                  users[autLev-1][userNo],
                  users[autLev-1][userNo].concat(pwSuffix)));
        }

        return DatabasePropertyTestSetup.
            builtinAuthentication(usersSuite, users[autLev-1], pwSuffix);
    }


    /**
     * Test database shutdown power enforcement
     *
     * @throws SQLException
     */
    public void testShutDown() throws SQLException
    {
        println("testShutDown: auth=" + this._authLevel +
                " user="+getTestConfiguration().getUserName());

        // make sure db is booted
        getConnection().close();

        String user = getTestConfiguration().getUserName();
        String password = getTestConfiguration().getUserPassword();

        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(
            ds, "connectionAttributes", "shutdown=true");
        JDBCDataSource.setBeanProperty(ds, "user", user);
        JDBCDataSource.setBeanProperty(ds, "password", password);
        try {
            ds.getConnection();
            fail("shutdown failed: no exception");
        } catch (SQLException e) {
            if ("08006".equals(e.getSQLState())) {
                // reboot if shutdown succeeded
                JDBCDataSource.setBeanProperty(ds, "connectionAttributes", "");
                ds.getConnection().close();
            }

            vetShutdownException(user, e);
        }
    }

    /**
     * Decide if the result of trying to shut down the database is
     * compliant with the semantics introduced by DERBY-2264.
     *
     */
    private void vetShutdownException (String user, SQLException e)
    {
        switch (_authLevel) {
        case NOAUTHENTICATION:
            assertSQLState("database shutdown, no authentication",
                           "08006", e);
            break;
        case AUTHENTICATION:
            /* We don't enforce dbo powers if only connection level
             * authentication is used, for now. This leniency was
             * introduced late in 10.3 release cycle for compatibility
             * reasons.
             */
            assertSQLState("database shutdown, authentication",
                           "08006", e);
            break;
        case SQLAUTHORIZATION:
            if ("TEST_DBO".equals(user)) {
                assertSQLState("database shutdown, SQL authorization, db owner",
                               "08006", e);
            } else {
                assertSQLState("database shutdown restriction, " +
                               "SQL authorization, not db owner",
                               "08004", e);
            }
            break;
        default:
            fail("test error: invalid authLevel: " + _authLevel);
            break;
        }
    }

    /**
     *
     * Construct suite of tests for database encryption action
     *
     * @param framework Derby framework name
     * @return A suite containing the test case for encryption
     * incarnated for the three security levels no authentication,
     * authentication, and authentication plus sqlAuthorization, The
     * latter two has an instance for dbo, and one for an ordinary user,
     * so there are in all five incarnations of tests.
     */
    private static Test dboEncryptionSuite(String framework)
    {
        Test tests[] = new Test[SQLAUTHORIZATION+1]; // one per authLevel

        /* Tests without any authorization active (level ==
         * NOAUTHENTICATION).  Note use of no shutdown decorator
         * variants: Necessary since framework doesn't know
         * bootPassword.
         */
        TestSuite noauthSuite =
            new TestSuite("suite: security level=" +
                          secLevelNames[NOAUTHENTICATION]);

        for (int tNo = 0; tNo < encryptionTests.length; tNo++) {
            noauthSuite.addTest(
                TestConfiguration.singleUseDatabaseDecoratorNoShutdown(
                    new DboPowersTest(encryptionTests[tNo], NOAUTHENTICATION,
                                      "foo", "bar")));
        }

        tests[NOAUTHENTICATION] = noauthSuite;

        /* Tests with authentication and sql authorization
         */
        for (int autLev = AUTHENTICATION;
             autLev <= SQLAUTHORIZATION ; autLev++) {

            tests[autLev] = wrapEncryptionUserTests(autLev);
        }

        TestSuite suite = new TestSuite("dboPowers:"+framework);

        /* run tests with no authentication enabled */
        suite.addTest(tests[NOAUTHENTICATION]);

        /* run test for all users with only authentication enabled */
        suite.addTest(tests[AUTHENTICATION]);

        /* run test for all users with authentication and
         * sqlAuthorization enabled
         */
        suite.addTest(tests[SQLAUTHORIZATION]);

        return suite;
    }

    /**
     * Wraps the encryption fixtures in decorators to run with data
     * base owner and one other valid user.
     *
     * @param autLev security context to use
     */

    private static Test wrapEncryptionUserTests(int autLev)
    {
        // add decorator for different users authenticated
        TestSuite usersSuite =
            new TestSuite("usersSuite: security level=" +
                          secLevelNames[autLev]);

        // First decorate with users, then with authentication.  Note
        // use of no teardown / no shutdown decorator variants:
        // Necessary since framework doesnt know bootPassword
        for (int userNo = 0; userNo < users.length; userNo++) {
            for (int tNo = 0; tNo < encryptionTests.length; tNo++) {
                Test test = TestConfiguration.changeUserDecorator
                    (new DboPowersTest(encryptionTests[tNo],
                                       autLev,
                                       users[autLev-1][0], // dbo
                                       users[autLev-1][0].concat(pwSuffix)),
                     users[autLev-1][userNo],
                     users[autLev-1][userNo].concat(pwSuffix));
                test = DatabasePropertyTestSetup.builtinAuthenticationNoTeardown
                    (test, users[autLev-1], pwSuffix);
                if (autLev == AUTHENTICATION) {
                    test = TestConfiguration.
                        singleUseDatabaseDecoratorNoShutdown(test);
                } else {
                    test = TestConfiguration.
                        sqlAuthorizationDecoratorSingleUse(test);
                }
                usersSuite.addTest(test);
            }
        }
        return usersSuite;
    }

    /**
     * Enumerates the encryption tests
     */
    final static String[] encryptionTests = { "testEncrypt", "testReEncrypt" };

    /**
     * Test database encryption for an already created
     * database. Note: The test needs to shut down the database for
     * the single use decorators to work.
     *
     * @throws SQLException
     */
    public void testEncrypt() throws SQLException
    {
        println("testEncrypt: auth=" + this._authLevel +
                " user="+getTestConfiguration().getUserName());

        // make sure db is created
        getConnection().close();

        // shut down database in preparation for encryption
        bringDbDown();

        // make encryption attempt
        String user = getTestConfiguration().getUserName();
        String password = getTestConfiguration().getUserPassword();
        String bootPassword="12345678";
        DataSource ds = JDBCDataSource.getDataSource();

        JDBCDataSource.setBeanProperty(ds, "connectionAttributes",
                                       "dataEncryption=true;bootPassword=" +
                                           bootPassword);
        JDBCDataSource.setBeanProperty(ds, "user", user);
        JDBCDataSource.setBeanProperty(ds, "password", password);

        Connection con = null;
        try {
            con = ds.getConnection();
            vetEncryptionAttempt(user, null);
        } catch (SQLException e) {
            vetEncryptionAttempt(user, e);
            bringDbDown();
            return;
        }

        try {
            derby3038(con);
        } catch (SQLException e) {
            fail("derby3038 regression: " + e);
        } 

        // we managed to encrypt: bring db down and up again to verify
        bringDbDown();
        bringDbUp(bootPassword);
        bringDbDown();
    }


    /**
     * Test database re-encryption for an already encrypted
     * database. Note: The test needs to shut down database for the
     * single use decorators to work.
     *
     * @throws SQLException
     */
    public void testReEncrypt() throws SQLException
    {
        println("testReEncrypt: auth=" + this._authLevel +
                " user="+getTestConfiguration().getUserName());

        // make sure db is created
        getConnection().close();

        // shut down database in preparation for encryption
        bringDbDown();

        String bootPassword="12345678";
        doEncrypt(bootPassword);
        bringDbDown();

        // make re-encryption attempt
        String user = getTestConfiguration().getUserName();
        String password = getTestConfiguration().getUserPassword();
        String newBootPassword="87654321";
        DataSource ds = JDBCDataSource.getDataSource();

        JDBCDataSource.setBeanProperty(ds, "connectionAttributes",
                                       "bootPassword=" + bootPassword +
                                       ";newBootPassword=" + newBootPassword);
        JDBCDataSource.setBeanProperty(ds, "user", user);
        JDBCDataSource.setBeanProperty(ds, "password", password);

        try {
            ds.getConnection();
            vetEncryptionAttempt(user, null);
        } catch (SQLException e) {
            vetEncryptionAttempt(user, e);
            bringDbDown();
            return;
        }

        // we managed to encrypt: bring db down and up again to verify
        bringDbDown();
        bringDbUp(newBootPassword);
        bringDbDown();
    }


    /**
     * Encrypt database, as owner (not testing encryption power here)
     * @param bootPassword
     * @throws SQLException
     */
    private void doEncrypt(String bootPassword) throws SQLException
    {
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(ds, "connectionAttributes",
                                       "dataEncryption=true;bootPassword=" +
                                       bootPassword);
        JDBCDataSource.setBeanProperty(ds, "user", _dbo);
        JDBCDataSource.setBeanProperty(ds, "password", _dboPassword);
        ds.getConnection();
    }


    /**
     * Shut down database, as db owner (not testing that power here)
     */
    private void bringDbDown()
    {
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(
            ds, "connectionAttributes", "shutdown=true");
        JDBCDataSource.setBeanProperty(ds, "user", _dbo);
        JDBCDataSource.setBeanProperty(ds, "password", _dboPassword);
        try {
            ds.getConnection();
            fail("shutdown failed: expected exception");
        } catch (SQLException e) {
            assertSQLState("database shutdown", "08006", e);
        }
    }


    /**
     * Boot database back up after encryption using current user,
     * should succeed
     *
     * @param bootPassword Boot using this bootPassword
     * @throws SQLException
     */
    private void bringDbUp(String bootPassword) throws SQLException
    {
        String user = getTestConfiguration().getUserName();
        String password = getTestConfiguration().getUserPassword();
        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(
            ds, "connectionAttributes", "bootPassword=" + bootPassword);
        JDBCDataSource.setBeanProperty(ds, "user", user);
        JDBCDataSource.setBeanProperty(ds, "password", password);
        ds.getConnection().close();
    }

    /**
     * Decide if the result of trying to (re)encrypt the database is
     * compliant with the semantics introduced by DERBY-2264.
     *
     * @param user The db user under which we tried to encrypt
     * @param e    Exception caught during attempt, if any
     */
    private void vetEncryptionAttempt (String user, SQLException e)
    {
        vetAttempt(user, e, "08004", "(re)encryption");
    }

    /**
     *
     * Construct suite of tests for hard upgrade database action
     *
     * NOTE: there is no real upgrade going on here since the
     * database is created with the same version, but the checking
     * is performed nonetheless, which is what we are testing
     * here.  This saves us from having to create a database with
     * an old version of Derby to test this power.
     *
     * @param framework Derby framework name
     * @return A suite containing the test case for hard upgrade
     * incarnated for the three security levels no authentication,
     * authentication, and authentication plus sqlAuthorization, The
     * latter two has an instance for dbo, and one for an ordinary user,
     * so there are in all five incarnations of tests.
     */
    private static Test dboHardUpgradeSuite(String framework)
    {
        Test tests[] = new Test[SQLAUTHORIZATION+1]; // one per authLevel

        /* Tests without any authorization active (level ==
         * NOAUTHENTICATION).
         */
        TestSuite noauthSuite =
            new TestSuite("suite: security level=" +
                          secLevelNames[NOAUTHENTICATION]);
        noauthSuite.addTest(new DboPowersTest("testHardUpgrade",
                                              NOAUTHENTICATION,
                                              "foo", "bar"));
        tests[NOAUTHENTICATION] = noauthSuite;

        /* First decorate with users, then with authentication. Do this
         * twice, once for authentication only, and once for
         * authentication + sqlAuthorization (see extra decorator
         * added below).
         */
        for (int autLev = AUTHENTICATION;
             autLev <= SQLAUTHORIZATION ; autLev++) {

            tests[autLev] = wrapHardUpgradeUserTests(autLev);
        }

        TestSuite suite = new TestSuite("dboPowers:"+framework);

        // A priori, doing a hard upgrade is a no-op here; we are only
        // interested in checking if we have the powers to do it. However,
        // sometimes the regression suite is run against a default database
        // (system/wombat) created by an earlier release to check soft upgrade
        // modes. To avoid interfering with such usage, we use a
        // singleUseDatabaseDecorator below, so we avoid accidentally hard
        // upgrading system/wombat in such runs. The SQLAUTHORIZATION run takes
        // care of itself since it uses another database anyway.

        /* run tests with no authentication enabled */
        suite.addTest(TestConfiguration.singleUseDatabaseDecorator(
                          tests[NOAUTHENTICATION]));

        /* run test for all users with only authentication enabled */
        suite.addTest(TestConfiguration.singleUseDatabaseDecorator(
                          tests[AUTHENTICATION]));

        /* run test for all users with authentication and
         * sqlAuthorization enabled
         */
        suite.addTest(
            TestConfiguration.
            sqlAuthorizationDecorator(tests[SQLAUTHORIZATION]));

        return suite;
    }

    /**
     * Wraps the shutdown fixture in decorators to run with data
     * base owner and one other valid user.
     *
     * @param autLev security context to use
     */

    private static Test wrapHardUpgradeUserTests(int autLev)
    {
        // add decorator for different users authenticated
        TestSuite usersSuite =
            new TestSuite("usersSuite: security level=" +
                          secLevelNames[autLev]);

        // First decorate with users, then with
        for (int userNo = 0; userNo < users.length; userNo++) {
            usersSuite.addTest
                (TestConfiguration.changeUserDecorator
                 (new DboPowersTest("testHardUpgrade",
                                    autLev,
                                    users[autLev-1][0], // dbo
                                    users[autLev-1][0].concat(pwSuffix)),
                  users[autLev-1][userNo],
                  users[autLev-1][userNo].concat(pwSuffix)));
        }

        return DatabasePropertyTestSetup.
            builtinAuthentication(usersSuite, users[autLev-1], pwSuffix);
    }

    /**
     * Test database upgrade power enforcement
     *
     * @throws SQLException
     */
    public void testHardUpgrade() throws SQLException
    {
        println("testHardUpgrade: auth=" + this._authLevel +
                " user="+getTestConfiguration().getUserName());

        // make sure db is created
        getConnection().close();
        // shut it down in preparation for upgrade boot
        bringDbDown();

        String user = getTestConfiguration().getUserName();
        String password = getTestConfiguration().getUserPassword();

        DataSource ds = JDBCDataSource.getDataSource();
        JDBCDataSource.setBeanProperty(
            ds, "connectionAttributes", "upgrade=true");
        JDBCDataSource.setBeanProperty(ds, "user", user);
        JDBCDataSource.setBeanProperty(ds, "password", password);
        try {
            ds.getConnection();
            vetHardUpgradeAttempt(user, null);
        } catch (SQLException e) {
            vetHardUpgradeAttempt(user, e);
        }

        bringDbDown();
    }


    /**
     * Decide if the result of trying to hard upgrade the database is
     * compliant with the semantics introduced by DERBY-2264.
     *
     * @param user The db user under which we tried to upgrade
     * @param e    Exception caught during attempt, if any
     */
    private void vetHardUpgradeAttempt (String user, SQLException e)
    {
        vetAttempt(user, e, "08004", "hard upgrade");
    }

    /**
     * Decide if the result of trying operation yields expected result.
     *
     * @param user The db user under which we tried to upgrade
     * @param e    Exception caught during attempted operation, if any
     * @param state The expected SQL state if this operation fails due to
     *             insufficient power
     * @param operation string describing the operation attempted
     */
    private void vetAttempt (String user, SQLException e,
                             String state, String operation)
    {
        switch (_authLevel) {
        case NOAUTHENTICATION:
            assertEquals(operation + ", no authentication", null, e);
            break;
        case AUTHENTICATION:
            /* We don't enforce dbo powers if only connection level
             * authentication is used, for now. This leniency was
             * introduced late in 10.3 release cycle for compatibility
             * reasons.
             */
            assertEquals(operation + ", authentication", null, e);
            break;
        case SQLAUTHORIZATION:
            if ("TEST_DBO".equals(user)) {
                assertEquals(operation + ", SQL authorization, db owner",
                             null, e);
            } else {
                assertSQLState(operation +", SQL authorization, not db owner",
                               state, e);
            }
            break;
        default:
            fail("test error: invalid authLevel: " + _authLevel);
            break;
        }
    }


    /**
     * Make and call a stored procedure which opens a nested
     * connection to expose DERBY-3038.
     */
    private void derby3038(Connection con) throws SQLException {

        java.sql.Statement s = con.createStatement();

        try {
            s.executeUpdate
                ("CREATE PROCEDURE DERBY3038PROC () " + 
                 "LANGUAGE JAVA PARAMETER STYLE JAVA EXTERNAL NAME '" +
                 DboPowersTest.class.getName() + ".derby3038Proc' " + 
                 "READS SQL DATA");
            s.executeUpdate("CALL DERBY3038PROC()");
        } finally {
            s.close();
        }
    }


    public static void derby3038Proc() 
        throws SQLException {

        // Before fixing DERNY-3038 this connect would fail.
        Connection con = java.sql.DriverManager.
            getConnection("jdbc:default:connection");
        con.close();
    }
}
