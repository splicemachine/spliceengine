/*
 * ddlUtils is a subproject of the Apache DB project, and is licensed under
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package org.apache.ddlutils;

import static org.apache.ddlutils.testutils.TestUtils.DRIVERCLASSNAME;
import static org.apache.ddlutils.testutils.TestUtils.PASSWORD;
import static org.apache.ddlutils.testutils.TestUtils.URL;
import static org.apache.ddlutils.testutils.TestUtils.USERNAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.dbcp.BasicDataSource;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

/**
 * TODO: JC - 8/26/16
 */
@Ignore
public class DumpAuthorizationIT {

    private static Map<String, BasicDataSource> dataSourceMap = new HashMap<>();
    private static BasicDataSource superDatasource;

    @BeforeClass
    public static void beforeClass() throws Exception {
        superDatasource = new BasicDataSource();
        superDatasource.setDriverClassName(DRIVERCLASSNAME);
        superDatasource.setUrl(URL);
        superDatasource.setUsername(USERNAME);
        superDatasource.setPassword(PASSWORD);
    }

    @AfterClass
    public static void afterClass() throws Exception {
        for (BasicDataSource dataSource : dataSourceMap.values()) {
            dataSource.close();
        }
        if (superDatasource != null) {
//            cleanup();
            superDatasource.close();
        }
    }

    @Test
    public void testMinimumSelectPrivilegeRequirement() throws SQLException {
        createUser("user1", "user1Pwd");
        Connection user1 = openUserConnection("user1", "user1Pwd");
        Statement user1St = user1.createStatement();

        createUser("user2", "user2Pwd");
        Connection user2 = openUserConnection("user2", "user2Pwd");
        Statement user2St = user2.createStatement();

        ResultSet rs = null;

        //user1 creates table t4191 and t4191_table2
        user1St.executeUpdate("create table t4191(x int, y int)");
        user1St.executeUpdate("create table t4191_table2(z int)");
        user1St.executeUpdate("create table t4191_table3(c31 int, c32 int)");
        user1St.executeUpdate("create view view_t4191_table3(v31, v32) " +
                                  "as select c31, c32 from t4191_table3");

        user1St.execute("grant update on t4191_table3 to public");
        user1St.execute("grant insert on t4191_table3 to public");
        user1St.execute("grant delete on t4191_table3 to public");
        //none of following DMLs will work because there is no select
        //privilege available on the view to user2.
        assertStatementError("42502", user2St, "update user1.t4191_table3 "+
            "set c31 = ( select max(v31) from user1.view_t4191_table3 )");
        assertStatementError("42502", user2St, "update user1.t4191_table3 "+
            "set c31 = ( select count(*) from user1.view_t4191_table3 )");
        assertStatementError("42502", user2St, "update user1.t4191_table3 "+
            "set c31 = ( select 1 from user1.view_t4191_table3 )");
        //Following should succeed
        user2St.execute("delete from user1.t4191_table3");

        //Grant select privilege on view so the above DMLs will start working
        user1St.execute("grant select on view_t4191_table3 to public");
        user2St.execute("update user1.t4191_table3 "+
                            "set c31 = ( select max(v31) from user1.view_t4191_table3 )");
        user2St.execute("update user1.t4191_table3 "+
                            "set c31 = ( select count(*) from user1.view_t4191_table3 )");
        user2St.execute("update user1.t4191_table3 "+
                            "set c31 = ( select 1 from user1.view_t4191_table3 )");

        //none of following selects will work because there is no select
        //privilege available to user2 yet.
        assertStatementError("42500", user2St, "select count(*) from user1.t4191");
        assertStatementError("42500", user2St, "select count(1) from user1.t4191");
        assertStatementError("42502", user2St, "select count(y) from user1.t4191");
        assertStatementError("42500", user2St, "select 1 from user1.t4191");
        //update below should fail because user2 does not have update
        //privileges on user1.t4191
        assertStatementError("42502", user2St, "update user1.t4191 set x=0");
        //update with subquery should fail too
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
            " ( select max(x) + 2 from user1.t4191 )");
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
            " ( select z from user1.t4191_table2 )");

        //grant select on user1.t4191(x) to user2 and now the above select
        //statements will work
        user1St.execute("grant select(x) on t4191 to user2");
        String[][] expRS = new String [][]
            {
                {"0"}
            };
//        rs = user2St.executeQuery("select count(*) from user1.t4191");
//        assertFullResultSet(rs, expRS, true, true);
//        rs = user2St.executeQuery("select count(1) from user1.t4191");
//        assertFullResultSet(rs, expRS, true, true);
//        rs = user2St.executeQuery("select 1 from user1.t4191");
//        assertQueryCount(rs, 0);

        //user2 does not have select privilege on 2nd column from user1.t4191
        assertStatementError("42502", user2St, "select count(y) from user1.t4191");
        //user2 does not have any select privilege on user1.table t4191_table2
//        assertStatementError("42500", user2St, "select x from user1.t4191_table2, user1.t4191");

        //grant select privilege on a column in user1.table t4191_table2 to user2
        user1St.execute("grant select(z) on t4191_table2 to user2");
        //now the following should run fine without any privilege issues
        rs = user2St.executeQuery("select x from user1.t4191_table2, user1.t4191");
        assertQueryCount(rs, 0);

        //revoke some column level privileges from user2
        user1St.execute("revoke select(x) on t4191 from user2");
        user1St.execute("revoke select(z) on t4191_table2 from user2");
        //update below should fail because user2 does not have update
        //privileges on user1.t4191
        assertStatementError("42502", user2St, "update user1.t4191 set x=0");
        //update with subquery should fail too
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
            " ( select max(x) + 2 from user1.t4191 )");
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
            " ( select z from user1.t4191_table2 )");
        //grant update on user1.t4191 to user2
        user1St.execute("grant update on t4191 to user2");
        //following update will now work because it has the required update
        //privilege
        assertUpdateCount(user2St, 0, "update user1.t4191 set x=0");
        //folowing will still fail because there is no select privilege on
        //user1.t4191(x)
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
            " ( select max(x) + 2 from user1.t4191 )");
        //following update will fail because there is no select privilege
        //on user1.t4191_table2
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
            " ( select z from user1.t4191_table2 )");
        user1St.execute("grant select(y) on t4191 to user2");
        //folowing will still fail because there is no select privilege on
        //user1.t4191(x)
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
            " ( select max(x) + 2 from user1.t4191 )");
        user1St.execute("grant select(x) on t4191 to user2");
        //following will now work because we have all the required privileges
        assertUpdateCount(user2St, 0, "update user1.t4191 set x=" +
            " ( select max(x) + 2 from user1.t4191 )");
        //folowing will still fail because there is no select privilege on
        //user1.t4191(x)
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
            " ( select z from user1.t4191_table2 )");
        user1St.execute("grant select on t4191_table2 to user2");
        //following will now pass
        assertUpdateCount(user2St, 0, "update user1.t4191 set x=" +
            " ( select z from user1.t4191_table2 )");

        //take away select privilege from one column and grant privilege on
        //another column in user1.t4191 to user2
        user1St.execute("revoke select(x) on t4191 from user2");
        //the following update will work because we still have update
        //privilege granted to user2
        assertUpdateCount(user2St, 0, "update user1.t4191 set x=0");
        //but following update won't work because there are no select
        //privileges available to user2 on user1.t4191(x)
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
            " ( select max(x) + 2 from user1.t4191 )");
        user1St.execute("grant select(y) on t4191 to user2");
        //following update still won't work because the select is granted on
        //user1.t4191(y) and not user1.t4191(x)
        assertStatementError("42502", user2St, "update user1.t4191 set x=" +
            " ( select max(x) + 2 from user1.t4191 )");
        //following queries will still work because there is still a
        //select privilege on user1.t4191 available to user2
        rs = user2St.executeQuery("select count(*) from user1.t4191");
        assertFullResultSet(rs, expRS, true, true);
        rs = user2St.executeQuery("select count(1) from user1.t4191");
        assertFullResultSet(rs, expRS, true, true);
        rs = user2St.executeQuery("select 1 from user1.t4191");
        assertQueryCount(rs, 0);
        rs = user2St.executeQuery("select count(y) from user1.t4191");
        assertFullResultSet(rs, expRS, true, true);
        //grant select privilege on user1.t4191(x) back to user2 so following
        //update can succeed
        user1St.execute("grant select(x) on t4191 to user2");
        assertUpdateCount(user2St, 0, "update user1.t4191 set x=" +
            " ( select max(x) + 2 from user1.t4191 )");

//        user1St.execute("drop table t4191");
//        user1St.execute("drop table t4191_table2");
//        user1St.execute("drop view view_t4191_table3");
//        user1St.execute("drop table t4191_table3");
//        user1St.execute("drop schema user1 restrict");
        user1.close();
        user2.close();
    }

    public static void cleanup() throws SQLException {
        try (Connection connection = superDatasource.getConnection()) {
            try (Statement st = connection.createStatement() ) {
                st.execute("drop table if exists user1.t4191");
                st.execute("drop table if exists user1.t4191_table2");
                st.execute("drop view user1.view_t4191_table3");
                st.execute("drop table if exists user1.t4191_table3");
                st.execute("drop schema user1 restrict");
            }
        }
    }

    //======================================================================================================

    private void createUser(String userName, String password) throws SQLException {
        dropUser(userName);
        try (Connection connection = superDatasource.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("call syscs_util.syscs_create_user(?,?)")){
                statement.setString(1, userName);
                statement.setString(2, password);
                statement.execute();
            }
        }
    }

    public void dropUser(String userName) throws SQLException {
        try (Connection connection = superDatasource.getConnection()) {
            try (PreparedStatement statement = connection.prepareStatement("select username from sys.sysusers where username = ?")){
                statement.setString(1, userName.toUpperCase());
                ResultSet rs = statement.executeQuery();
                if (rs.next()) {
                    try (PreparedStatement preparedStatement = connection.prepareStatement("call syscs_util.syscs_drop_user(?)")) {
                        preparedStatement.setString(1, userName);
                        preparedStatement.execute();
                    }
                }
            }
        }
    }


    private static void assertFullResultSet(
        ResultSet rs,
        Object [][] expectedRows,
        boolean allAsTrimmedStrings,
        boolean closeResultSet)
        throws SQLException
    {
        int rows;
        ResultSetMetaData rsmd = rs.getMetaData();

        // Assert that we have the right number of columns. If we expect an
        // empty result set, the expected column count is unknown, so don't
        // check.
        if (expectedRows.length > 0) {
            assertEquals("Unexpected column count:",
                                expectedRows[0].length, rsmd.getColumnCount());
        }

        for (rows = 0; rs.next(); rows++)
        {


            /* If we have more actual rows than expected rows, don't
             * try to assert the row.  Instead just keep iterating
             * to see exactly how many rows the actual result set has.
             */
            if (rows < expectedRows.length)
            {
                assertRowInResultSet(rs, rows + 1,
                                     expectedRows[rows], allAsTrimmedStrings);
            }
        }

        if ( closeResultSet ) { rs.close(); }

        // And finally, assert the row count.
        assertEquals("Unexpected row count:", expectedRows.length, rows);
    }

    private static void assertRowInResultSet(ResultSet rs,
                                             int rowNum, Object [] expectedRow, boolean asTrimmedStrings) throws SQLException
    {
        int cPos = 0;
        ResultSetMetaData rsmd = rs.getMetaData();
        for (int i = 0; i < expectedRow.length; i++)
        {

            Object obj;
            if (asTrimmedStrings)
            {
                // Trim the expected value, if non-null.
                if (expectedRow[i] != null)
                    expectedRow[i] = ((String)expectedRow[i]).trim();

                /* Different clients can return different values for
                 * boolean columns--namely, 0/1 vs false/true.  So in
                 * order to keep things uniform, take boolean columns
                 * and get the JDBC string version.  Note: since
                 * Derby doesn't have a BOOLEAN type, we assume that
                 * if the column's type is SMALLINT and the expected
                 * value's string form is "true" or "false", then the
                 * column is intended to be a mock boolean column.
                 */
                if ((expectedRow[i] != null)
                    && (rsmd.getColumnType(cPos) == Types.SMALLINT))
                {
                    String s = expectedRow[i].toString();
                    if (s.equals("true") || s.equals("false"))
                        obj = (rs.getShort(cPos) == 0) ? "false" : "true";
                    else
                        obj = rs.getString(cPos);

                }
                else
                {
                    obj = rs.getString(cPos);

                }

                // Trim the rs string.
                if (obj != null)
                    obj = ((String)obj).trim();

            }
            else
                obj = rs.getObject(cPos);

            boolean ok = (rs.wasNull() && (expectedRow[i] == null))
                || (!rs.wasNull()
                && (expectedRow[i] != null)
                && (expectedRow[i].equals(obj)
                || (obj instanceof byte[] // Assumes byte arrays
                && Arrays.equals((byte[] )obj,
                                 (byte[] )expectedRow[i]))));
            if (!ok)
            {
                Object expected = expectedRow[i];
                fail("Column value mismatch @ column '" +
                                rsmd.getColumnName(cPos) + "', row " + rowNum +
                                ":\n    Expected: >" + expected +
                                "<\n    Found:    >" + obj + "<");
            }

            if (rs.wasNull())
                assertFalse(rsmd.isNullable(cPos) == ResultSetMetaData.columnNoNulls);
        }
    }


    private Connection openUserConnection(String user, String password) throws SQLException {
        BasicDataSource dataSource = dataSourceMap.get(user);
        if (dataSource == null) {
            dataSource = new BasicDataSource();
            dataSource.setDriverClassName(DRIVERCLASSNAME);
            dataSource.setUrl(URL);
            dataSource.setUsername(user);
            dataSource.setPassword(password);
            dataSourceMap.put(user, dataSource);
        }
        return dataSource.getConnection();
    }

    private void assertStatementError(String expectedState, Statement st, String query) {
        try {
            st.execute(query);
        } catch (SQLException se) {
            String state = se.getSQLState();
            assertEquals("Unexpected state: " + state+": "+se.getLocalizedMessage(), expectedState, state);
            return;
        }
        fail("Expected state"+expectedState );
    }

    private void assertUpdateCount(Statement statement, int expectedCnt, String sql) throws SQLException {
        assertEquals("Update count does not match:",
                     expectedCnt, statement.executeUpdate(sql));

    }

    private static int assertQueryCount(ResultSet rs, int expectedRows) throws SQLException {
            ResultSetMetaData rsmd = rs.getMetaData();

            int rows = 0;
            while (rs.next()) {
                for (int col = 1; col <= rsmd.getColumnCount(); col++)
                {
                    String s = rs.getString(col);
                    Assert.assertEquals(s == null, rs.wasNull());
                    if (rs.wasNull())
                        assertFalse(rsmd.isNullable(col) == ResultSetMetaData.columnNoNulls);
                }
                rows++;
            }
            rs.close();

            if (expectedRows >= 0)
                Assert.assertEquals("Unexpected row count:", expectedRows, rows);

            return rows;

    }

}
