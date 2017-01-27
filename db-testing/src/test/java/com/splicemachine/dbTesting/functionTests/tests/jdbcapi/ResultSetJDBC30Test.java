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

import com.splicemachine.dbTesting.junit.BaseJDBCTestCase;

import java.sql.*;
import junit.framework.Test;
import junit.framework.TestSuite;
import com.splicemachine.dbTesting.junit.JDBC;
import com.splicemachine.dbTesting.junit.TestConfiguration;

/**
 * Test of additional methods in JDBC3.0 result set
 */
public class ResultSetJDBC30Test extends BaseJDBCTestCase {

    /** Creates a new instance of ResultSetJDBC30Test */
    public ResultSetJDBC30Test(String name) {
        super(name);
    }

    /**
     * Set up the connection to the database.
     */
    public void setUp() throws  Exception {
        getConnection().setAutoCommit(false);
        Statement stmt = createStatement();
        stmt.execute("create table t (i int, s smallint, r real, "+
            "d double precision, dt date, t time, ts timestamp, "+
            "c char(10), v varchar(40) not null, dc dec(10,2))");
        stmt.execute("insert into t values(1,2,3.3,4.4,date('1990-05-05'),"+
                     "time('12:06:06'),timestamp('1990-07-07 07:07:07.07'),"+
                     "'eight','nine', 11.1)");
        stmt.close();
        commit();
    }

    protected void tearDown() throws Exception {
        Statement stmt = createStatement();
        stmt.executeUpdate("DROP TABLE t");
        commit();
        super.tearDown();
    }

    public static Test suite() {
        return TestConfiguration.defaultSuite(ResultSetJDBC30Test.class);
    }


    public void testNotImplementedMethods() throws Exception {
        Statement stmt = createStatement();

        ResultSet rs = stmt.executeQuery("select * from t");
        assertTrue("FAIL - row not found", rs.next());

        try {
            rs.getURL(8);
            fail("FAIL - Shouldn't reach here. Method not implemented" +
                    " yet.");
        } catch (SQLException se) {
            assertSQLState(NOT_IMPLEMENTED, se);
        }
        try {
            rs.getURL("c");
            fail("FAIL - Shouldn't reach here. Method not implemented" +
                    " yet.");
        } catch (SQLException se) {
            assertSQLState(NOT_IMPLEMENTED, se);
        }
        try {
            rs.updateRef(8, null);
            fail("FAIL - Shouldn't reach here. Method not implemented" +
                    " yet.");
        } catch (SQLException se) {
            assertSQLState(NOT_IMPLEMENTED, se);
        } catch (NoSuchMethodError nsme) {
            assertTrue("FAIL - ResultSet.updateRef not present - correct for" +
                    " JSR169", JDBC.vmSupportsJSR169());
        }
        try {
            rs.updateRef("c", null);
            fail("FAIL - Shouldn't reach here. Method not implemented" +
                    " yet.");
        } catch (SQLException se) {
            assertSQLState(NOT_IMPLEMENTED, se);
        } catch (NoSuchMethodError nsme) {
            assertTrue("FAIL - ResultSet.updateRef not present - correct for" +
                    " JSR169", JDBC.vmSupportsJSR169());
        }
        try {
            rs.updateBlob(8, (Blob)null);
            if (usingEmbedded()) {
                fail("FAIL - Shouldn't reach here. Method is being invoked" +
                        " on a read only resultset.");
            } else {
                fail("FAIL - Shouldn't reach here. Method not implemented" +
                        " yet.");
            }
        } catch (SQLException se) {
            assertSQLState(UPDATABLE_RESULTSET_API_DISALLOWED, se);
        }
        try {
            rs.updateBlob("c", (Blob)null);
            if (usingEmbedded()) {
                fail("FAIL - Shouldn't reach here. Method is being invoked" +
                        " on a read only resultset.");
            } else {
                fail("FAIL - Shouldn't reach here. Method not implemented" +
                        " yet.");
            }
        } catch (SQLException se) {
            assertSQLState(UPDATABLE_RESULTSET_API_DISALLOWED, se);
        }
        try {
            rs.updateClob(8, (Clob)null);
            if (usingEmbedded()) {
                fail("FAIL - Shouldn't reach here. Method is being invoked" +
                        " on a read only resultset.");
            } else {
                fail("FAIL - Shouldn't reach here. Method not implemented" +
                        " yet.");
            }
        } catch (SQLException se) {
            assertSQLState(UPDATABLE_RESULTSET_API_DISALLOWED, se);
        }
        try {
            rs.updateClob("c", (Clob)null);
            if (usingEmbedded()) {
                fail("FAIL - Shouldn't reach here. Method is being invoked" +
                        " on a read only resultset.");
            } else {
                fail("FAIL - Shouldn't reach here. Method not implemented" +
                        " yet.");
            }
        } catch (SQLException se) {
            assertSQLState(UPDATABLE_RESULTSET_API_DISALLOWED, se);
        }
        try {
            rs.updateArray(8, null);
            fail("FAIL - Shouldn't reach here. Method not implemented" +
                    " yet.");
        } catch (SQLException se) {
            assertSQLState(NOT_IMPLEMENTED, se);
        } catch (NoSuchMethodError nsme) {
            assertTrue("FAIL - ResultSet.updateArray not present - correct" +
                    " for JSR169", JDBC.vmSupportsJSR169());
        }
        try {
            rs.updateArray("c", null);
            fail("FAIL - Shouldn't reach here. Method not implemented" +
                    " yet.");
        } catch (SQLException se) {
            assertSQLState(NOT_IMPLEMENTED, se);
        } catch (NoSuchMethodError nsme) {
            assertTrue("FAIL - ResultSet.updateArray not present - correct" +
                    " for JSR169", JDBC.vmSupportsJSR169());
        }

        rs.close();
        stmt.close();
        commit();
    }

    public void testCloseResultSetAutoCommit() throws Exception {
        //
        // Check our behavior around closing result sets when auto-commit
        // is true.  Test with both holdable and non-holdable result sets
        //
        getConnection().setAutoCommit(true);

        // Create a non-updatable holdable result set, and then try to
        // update it
        getConnection().setHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT);
        Statement stmt = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);

        ResultSet rs = stmt.executeQuery("select * from t");
        rs.next();

        checkForCloseOnException(rs, true);

        rs.close();
        stmt.close();

        // Create a non-updatable non-holdable result set, and then try to
        // update it
        getConnection().setHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT);
        stmt = createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
            ResultSet.CONCUR_READ_ONLY);

        rs = stmt.executeQuery("select * from t");
        rs.next();

        checkForCloseOnException(rs, false);

        rs.close();
        stmt.close();
        commit();
    }

    private void checkForCloseOnException(ResultSet rs, boolean holdable)
            throws Exception
    {
        try {
            rs.updateBlob("c",(Blob)null);
            fail("FAIL - rs.updateBlob() on a read-only result set" +
                "should not have succeeded");
        } catch (SQLException ex) {}
        // The result set should not be closed on exception, this call should
        // not cause an exception
        rs.beforeFirst();
    }

    private static final String NOT_IMPLEMENTED = "0A000";
    private static final String UPDATABLE_RESULTSET_API_DISALLOWED = "XJ083";
}
