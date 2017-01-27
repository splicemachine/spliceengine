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

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import junit.framework.Test;
import junit.framework.TestSuite;

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;
import com.splicemachine.dbTesting.junit.BaseTestCase;
import com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup;
import com.splicemachine.dbTesting.junit.TestConfiguration;
import com.splicemachine.dbTesting.junit.Utilities;

public class LargeDataLocksTest extends BaseJDBCTestCase {

    public LargeDataLocksTest(String name) {
        super(name);
    }

    /**
     * Test that ResultSet.getCharacterStream does not hold locks after the
     * ResultSet is closed
     * 
     * @throws SQLException
     * @throws IOException
     */
    public void testGetCharacterStream() throws SQLException, IOException {
        // getCharacterStream() no locks expected after retrieval
        int numChars = 0;
        Statement stmt = createStatement();
        String sql = "SELECT bc from t1";
        // First with getCharacterStream
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        java.io.Reader characterStream = rs.getCharacterStream(1);
        // Extract all the characters
        int read = characterStream.read();
        while (read != -1) {
            read = characterStream.read();
            numChars++;
        }
        assertEquals(38000, numChars);
        rs.close();
        assertLockCount(0);
        commit();
    }

    /**
     * Verify that getBytes does not hold locks after ResultSet is closed.
     * 
     * @throws SQLException
     */
    public void testGetBytes() throws SQLException {
        // getBytes() no locks expected after retrieval
        Statement stmt = createStatement();
        String sql = "SELECT bincol from t1";
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        byte[] value = rs.getBytes(1);
        assertEquals(38000, value.length);
        rs.close();
        assertLockCount(0);
        commit();

    }

    /**
     * Verify that getBinaryStream() does not hold locks after retrieval
     * 
     * @throws SQLException
     * @throws IOException
     */
    public void testGetBinaryStream() throws SQLException, IOException {
        int numBytes = 0;
        getConnection().setAutoCommit(false);
        Statement stmt = createStatement();
        String sql = "SELECT bincol from t1";
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        InputStream stream = rs.getBinaryStream(1);
        int read = stream.read();
        while (read != -1) {
            read = stream.read();
            numBytes++;
        }
        assertEquals(38000, numBytes);
        rs.close();
        assertLockCount(0);
        commit();
    }

    /**
     * Test that ResultSet.getString() does not hold locks after the ResultSet
     * is closed
     * 
     * @throws SQLException
     * @throws IOException
     */
    public void testGetString() throws SQLException, IOException {
        // getString() no locks expected after retrieval
        Statement stmt = createStatement();
        String sql = "SELECT bc from t1";
        ResultSet rs = stmt.executeQuery(sql);
        rs.next();
        String value = rs.getString(1);
        assertEquals(38000, value.length());
        rs.close();
        assertLockCount(0);
        commit();
    }

    /**
     * Assert that the lock table contains a certain number of locks. Fail and
     * dump the contents of the lock table if the lock table does not contain
     * the expected number of locks.
     *
     * @param expected the expected number of locks
     */
    private void assertLockCount(int expected) throws SQLException {
        // Count the locks in a new connection so that we don't accidentally
        // make the default connection auto-commit and release locks.
        Connection conn = openDefaultConnection();
        Statement stmt = conn.createStatement();

        // First wait for post-commit work to complete so that we don't count
        // locks held by the background worker thread.
        stmt.execute("call wait_for_post_commit()");

        ResultSet rs = stmt.executeQuery("select * from syscs_diag.lock_table");
        ResultSetMetaData meta = rs.getMetaData();

        // Build an error message with the contents of the lock table as
        // we walk through it.
        StringBuffer msg = new StringBuffer(
                "Unexpected lock count. Contents of lock table:\n");
        int count;
        for (count = 0; rs.next(); count++) {
            msg.append(count + 1).append(": ");
            for (int col = 1; col <= meta.getColumnCount(); col++) {
                String name = meta.getColumnName(col);
                Object val = rs.getObject(col);
                msg.append(name).append('=').append(val).append(' ');
            }
            msg.append('\n');
        }

        rs.close();
        stmt.close();
        conn.close();

        assertEquals(msg.toString(), expected, count);
    }

    public static Test baseSuite(String name) {

        TestSuite suite = new TestSuite(name);
        suite.addTestSuite(LargeDataLocksTest.class);

        return new CleanDatabaseTestSetup(suite) {

            /**
             * Create and populate table
             * @see com.splicemachine.dbTesting.junit.CleanDatabaseTestSetup#decorateSQL(java.sql.Statement)
             */
            protected void decorateSQL(Statement s) throws SQLException {
                Connection conn = getConnection();
                conn.setAutoCommit(false);
                PreparedStatement ps = null;
                String sql;

                sql = "CREATE TABLE t1 (bc CLOB(1M), bincol BLOB(1M), datalen int)";
                s.executeUpdate(sql);

                // Insert big and little values
                sql = "INSERT into t1 values(?,?,?)";
                ps = conn.prepareStatement(sql);

                ps.setCharacterStream(1, new java.io.StringReader(Utilities
                        .repeatChar("a", 38000)), 38000);
                try {
                    ps.setBytes(2, Utilities.repeatChar("a", 38000).getBytes("US-ASCII"));
                } catch (UnsupportedEncodingException ue) {
                    // Shouldn't happen US-ASCII should always be supported
                    BaseTestCase.fail(ue.getMessage(), ue);
                }
                ps.setInt(3, 38000);
                ps.executeUpdate();
                ps.close();

                // Create a procedure for use by assertLockCount() to ensure
                // that the background worker thread has completed all the
                // post-commit work.
                s.execute("CREATE PROCEDURE WAIT_FOR_POST_COMMIT() "
                        + "LANGUAGE JAVA EXTERNAL NAME "
                        + "'com.splicemachine.dbTesting.functionTests.util."
                        + "T_Access.waitForPostCommitToFinish' "
                        + "PARAMETER STYLE JAVA");

                conn.commit();
            }
        };
    }

    public static Test suite() {
        TestSuite suite = new TestSuite("LargeDataLocksTest");
        suite.addTest(baseSuite("LargeDataLocksTest:embedded"));
        // Disable for client until DERBY-2892 is fixed
        suite.addTest(TestConfiguration.clientServerDecorator(baseSuite("LargeDataLocksTest:client")));
        return suite;

    }

}
