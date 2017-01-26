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

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * DERBY-4249 This class can be used as a framework to create junit Recovery
 * Test or converting harness Recovery Tests to junit tests.
 **/

public final class RecoveryTest extends BaseJDBCTestCase
{
    public RecoveryTest(String name)
    {
        super(name);
    }

    public static Test suite()
    {
        // Add the test case into the test suite
        TestSuite suite = new TestSuite("RecoveryTest");
        suite.addTest(decorateTest());
        return suite;
    }

    private static Test decorateTest()
    {
        Test test = new CleanDatabaseTestSetup(TestConfiguration.embeddedSuite(
                RecoveryTest.class));
        return test;
    }

    /**
     * Tests the recovery of database. The test achieves its purpose 
     * as follows:
     * Connect, create a table, commit and shutdown the database.
     * fork a jvm, add one row, commit, add another row, exit the jvm(killed).
     * Reconnect with the first jvm and verify that the first row is there 
     * and the second is not. 
     * When a new JVM connects, the log entries are read one by one and it 
     * then rolls back to the transaction boundaries, then the database is
     * in a consistent state. 
     * @throws Exception
     */
    public void testBasicRecovery() throws Exception
    {
        Connection c = getConnection();
        c.setAutoCommit(false);
        Statement st = createStatement();
        st.executeUpdate("create table t( i int )");
        c.commit();
        TestConfiguration.getCurrent().shutdownDatabase();
        st.close();
        c.close();

        //fork JVM
        assertLaunchedJUnitTestMethod("com.splicemachine.dbTesting.functionTests.tests.store.RecoveryTest.launchRecoveryInsert");

        st = createStatement();
        ResultSet rs = st.executeQuery("select i from t");
        JDBC.assertFullResultSet(rs, new String[][] { { "1956" } } );
    }

    /**
     * This fixture is used by the forked JVM to add and commit rows to the
     * database in the first JVM.  Note that this routine does not shutdown
     * the database, and thus executes a "dirty" shutdown.  This dirty 
     * shutdown is why we are forking the JVM so that we can test recovery
     * codepaths during the reboot of the database.
     *
     * Do not call TestConfiguration.getCurrent().shutdownDatabase(), as
     * that will do a clean shutdown and leave no work for restart recovery
     * to do.  The point of assertLaunchedJUnitTestMethod() is to launch
     * a separate process, crash it after some work, and then do restart
     * recovery to test those code paths that only get exercised when restart
     * starts on a non-cleanly shutdown database.
     *
     * @throws SQLException 
     **/
    public void launchRecoveryInsert() throws SQLException
    {
            Connection c = getConnection();
            c.setAutoCommit(false);
            Statement st = createStatement();
            st.executeUpdate("insert into t(i) values (1956)");
            c.commit();
            st.executeUpdate("insert into t(i) values (2011)");
    }
}
