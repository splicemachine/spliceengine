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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.DatabasePropertyTestSetup;
import com.splicemachine.dbTesting.junit.RuntimeStatisticsParser;
import com.splicemachine.dbTesting.junit.SQLUtilities;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Test setTransactionIsolation
 * 
 */
public class SetTransactionIsolationTest extends BaseJDBCTestCase {

    /**
     * @param name
     */
    public SetTransactionIsolationTest(String name) {
        super(name);
    }

    public static int[] isoLevels = { Connection.TRANSACTION_READ_UNCOMMITTED,
            Connection.TRANSACTION_REPEATABLE_READ,
            Connection.TRANSACTION_READ_COMMITTED,
            Connection.TRANSACTION_SERIALIZABLE };

    /**
     * test setting of isolation levels with and without lock timeouts
     * @throws SQLException
     */
    public void testIsolation() throws SQLException {
        Connection conn = getConnection();
        Connection conn2 = openDefaultConnection();
        conn.setAutoCommit(false);
        // test with no lock timeouts
        for (int i = 0; i < isoLevels.length; i++) {
            checkIsolationLevelNoTimeout(conn, isoLevels[i]);
        }
       
        // Now do an insert to create lock timeout
        Statement stmt = conn.createStatement();
        stmt.executeUpdate("insert into t1 values(4,'Fourth Hello')");
        for (int i = 0; i < isoLevels.length; i++)
            checkIsolationLevelTimeout(conn2, isoLevels[i]);

        stmt.close();

        // rollback to cleanup locks from insert
        conn.rollback();

    }

    /**
     * Check setTransactioIsolation and queries with timeout expected in
     * all cases except READ_UNCOMMITTED
     * 
     * @param conn     Connection to use
     * @param isoLevel Isolation level to test from Connection.TRANSACTION_*
     * @throws SQLException
     */
    private void checkIsolationLevelTimeout(Connection conn, int isoLevel)
            throws SQLException {

        RuntimeStatisticsParser rsp = null;
        conn.setTransactionIsolation(isoLevel);

        try {
            rsp = SQLUtilities.executeAndGetRuntimeStatistics(conn,
                    "select * from t1");
            // only READ_UNCOMMITTED should make it through
            assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, rsp
                    .getIsolationLevel());
        } catch (SQLException se) {
            if (isoLevel != Connection.TRANSACTION_READ_UNCOMMITTED)
                assertSQLState("expected lock timeout", "40XL1", se);
        }
        try {
            rsp = SQLUtilities.executeAndGetRuntimeStatistics(conn,
                    "insert into t1copy (select * from t1)");
            ;
            // only READ_UNCOMMITTED should make it through
            assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, rsp
                    .getIsolationLevel());
        } catch (SQLException se) {
            if (isoLevel != Connection.TRANSACTION_READ_UNCOMMITTED)
                assertSQLState("expected lock timeout", "40XL1", se);

        }
    }

    /**
     * Test setTransactionIsolation and queries with no timeout expected
     * @param conn 
     * @param isoLevel
     * @throws SQLException
     */
    private void checkIsolationLevelNoTimeout(Connection conn, int isoLevel)
            throws SQLException {

        conn.setTransactionIsolation(isoLevel);
        RuntimeStatisticsParser rsp = SQLUtilities
                .executeAndGetRuntimeStatistics(conn, "select * from t1");
        assertEquals(isoLevel, rsp.getIsolationLevel());

        rsp = SQLUtilities.executeAndGetRuntimeStatistics(conn,
                "insert into t1copy (select * from t1)");
        ;
        assertEquals(isoLevel, rsp.getIsolationLevel());

    }

    /**
     * setTransactionIsolation commits?
     */
    public void testSetTransactionIsolationCommitRollback() throws SQLException {
        Connection conn = getConnection();

        conn.rollback();
        conn.setAutoCommit(false);
        conn
                .setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE);
        Statement s = conn.createStatement();
        s.executeUpdate("delete from t3");
        s.executeUpdate("insert into t3 values(1)");
        conn.commit();
        s.executeUpdate("insert into t3 values(2)");
        conn
                .setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE);
        conn.rollback();
        ResultSet rs = s.executeQuery("select count(*) from t3");
        rs.next();
        int count = rs.getInt(1);
        assertEquals(1, count);
        rs.close();
        s.close();

    }

    /**
     * Call setTransactionIsolation with holdable cursor open?
     */
    public void testSetTransactionIsolationInHoldCursor() throws SQLException

    {
        Connection conn = getConnection();
        try {

            PreparedStatement ps = conn.prepareStatement("SELECT * from TAB1");
            ResultSet rs = ps.executeQuery();
            rs.next();
            // setTransactionIsolation should fail because we have
            // a holdable cursor open
            conn
                    .setTransactionIsolation(java.sql.Connection.TRANSACTION_SERIALIZABLE);
            rs.next(); // to fix DERBY-1108. Else the GC for ibm15 will clean
                        // up the ResultSet Object
        } catch (SQLException se) {
            assertSQLState("Expected Exception if held cursor is open",
                    "X0X03", se);
            return;
        }
        fail("FAIL: setTransactionIsolation() did not throw exception with open hold cursor");
    }

    public static Test baseSuite(String name) {

        TestSuite suite = new TestSuite(name);
        suite.addTestSuite(SetTransactionIsolationTest.class);

        // Some test cases expect lock timeouts, so reduce the timeout to
        // make the test go faster.
        Test test = DatabasePropertyTestSetup.setLockTimeouts(suite, 1, 3);

        return new CleanDatabaseTestSetup(test) {

            /**
             * Create and populate table
             * 
             * @see com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup#decorateSQL(java.sql.Statement)
             */
            protected void decorateSQL(Statement s) throws SQLException {
                Connection conn = getConnection();

                /**
                 * Creates the table used in the test cases.
                 * 
                 */
                
                final int stringLength = 400;
                s.executeUpdate("CREATE TABLE TAB1 (c11 int, " + "c12 varchar("
                        + stringLength + "))");
                PreparedStatement insertStmt = conn
                        .prepareStatement("INSERT INTO TAB1 VALUES(?,?)");
                // We need to ensure that there is more data in the table than
                // the
                // client can fetch in one message (about 32K). Otherwise, the
                // cursor might be closed on the server and we are not testing
                // the
                // same thing in embedded mode and client/server mode.
                final int rows = 40000 / stringLength;
                StringBuffer buff = new StringBuffer(stringLength);
                for (int i = 0; i < stringLength; i++) {
                    buff.append(" ");
                }
                for (int i = 1; i <= rows; i++) {
                    insertStmt.setInt(1, i);
                    insertStmt.setString(2, buff.toString());
                    insertStmt.executeUpdate();
                }
                insertStmt.close();

                s.execute("create table t1(I int, B char(15))");
                s.execute("create table t1copy(I int, B char(15))");

                s.executeUpdate("INSERT INTO T1 VALUES(1,'First Hello')");
                s.executeUpdate("INSERT INTO T1 VALUES(2,'Second Hello')");
                s.executeUpdate("INSERT INTO T1 VALUES(3,'Third Hello')");

                s.executeUpdate("create table t3 (i integer)");
            }

        };

    }

    public static Test suite() {
        TestSuite suite = new TestSuite("SetTransactionIsolation");

        suite.addTest(baseSuite("SetTransactionIsolation:embedded"));

        suite
                .addTest(TestConfiguration
                        .clientServerDecorator(baseSuite("SetTransactionIsolation:client")));
        return suite;

    }

}
