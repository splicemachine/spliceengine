/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute.operations.joins;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

/**
 * Integration tests for NestedLoopJoinOperation.
 */
//@Category(SerialTest.class) //in Serial category because of the NestedLoopIteratorClosesStatements test
public class NestedLoopJoinOperationIT extends SpliceUnitTest {

    private static final String SCHEMA = NestedLoopJoinOperationIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);
    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);
    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(schemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);


    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1(a1 int, b1 int, c1 int, primary key (a1))")
                .withInsert("insert into t1 values(?,?,?)")
                .withRows(rows(
                        row(1,1,1),
                        row(2,2,2),
                        row(3,3,3)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t2(a2 int, b2 int, c2 int, primary key (a2))")
                .withInsert("insert into t2 values(?,?,?)")
                .withRows(rows(
                        row(1,1,1),
                        row(2,2,2),
                        row(3,3,3)))
                .create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), schemaWatcher.toString());
    }

    /* Regression test for DB-1027 */
    @Test
    public void testCanJoinTwoTablesWithViewAndQualifiedSinkOperation() throws Exception {
        // B
        methodWatcher.executeUpdate("create table B(c1 int, c2 int, c3 char(1), c4 int, c5 int, c6 int)");
        // B2
        methodWatcher.executeUpdate("create table B2 (c1 int, c2 int, c3 char(1), c4 int, c5 int,c6 int)");
        methodWatcher.executeUpdate("insert into B2 (c5,c1,c3,c4,c6) values (3,4, 'F',43,23)");
        // B3
        methodWatcher.executeUpdate("create table B3(c8 int, c9 int, c5 int, c6 int)");
        methodWatcher.executeUpdate("insert into B3 (c5,c8,c9,c6) values (2,3,19,28)");
        // B4
        methodWatcher.executeUpdate("create table B4(c7 int, c4 int, c6 int)");
        methodWatcher.executeUpdate("insert into B4 (c7,c4,c6) values (4, 42, 31)");
        // VIEW
        methodWatcher.executeUpdate("create view bvw (c5,c1,c2,c3,c4) as select c5,c1,c2,c3,c4 from B2 union select c5,c1,c2,c3,c4 from B");

        ResultSet rs = methodWatcher.executeQuery("select B3.* from B3 join BVW on (B3.c8 = BVW.c5) join B4 on (BVW.c1 = B4.c7) where B4.c4 = 42");
        assertEquals("" +
                "C8 |C9 |C5 |C6 |\n" +
                "----------------\n" +
                " 3 |19 | 2 |28 |", toString(rs));
    }

    @Test
    public void joinEmptyRightSideOnConstant() throws Exception {
        methodWatcher.executeUpdate("create table DB4003(a int)");
        methodWatcher.executeUpdate("create table EMPTY_TABLE(e int)");
        methodWatcher.executeUpdate("insert into DB4003 values 1,2,3");
        ResultSet rs = methodWatcher.executeQuery("select count(*) from DB4003 join (select 1 r from EMPTY_TABLE) foo --SPLICE-PROPERTIES joinStrategy=NESTEDLOOP\n on TRUE");
        assertEquals("" +
                "1 |\n" +
                "----\n" +
                " 0 |", toString(rs));

    }

    @Test
    public void nestedLoopJoinIsSorted() throws Exception {
        methodWatcher.executeUpdate("create table DB5773(i int primary key)");
        methodWatcher.executeUpdate("create table b_nlj(i int)");
        methodWatcher.executeUpdate("create table c_mj(i int primary key)");
        try {
            methodWatcher.executeQuery("select * from --splice-properties joinOrder=fixed\n" +
                    "DB5773, b_nlj --splice-properties joinStrategy=nestedloop\n" +
                    ", c_mj --splice-properties joinStrategy=merge\n" +
                    "where DB5773.i = c_mj.i");
        } catch (SQLException e) {
            // Error no longer expected with DB-6453
            fail("Query should not error out");
        }

        // We shouldn't preserve ordering from the right table of a nested loop join
        try {
            methodWatcher.executeQuery("select * from --splice-properties joinOrder=fixed\n" +
                    "DB5773, DB5773 b_nlj --splice-properties joinStrategy=nestedloop\n" +
                    ", DB5773 c_mj --splice-properties joinStrategy=merge\n" +
                    "where b_nlj.i = c_mj.i");
            fail("Should have raised exception");
        } catch (SQLException e) {
            // Error expected due to invalid MERGE join:
            // ERROR 42Y69: No valid execution plan was found for this statement.
            assertEquals("42Y69", e.getSQLState());
        }
    }

    private String toString(ResultSet rs) throws Exception {
        return TestUtils.FormattedResult.ResultFactory.toString(rs);
    }

    // DB-4883 (Wells)
    // See JoinWithTrimIT for additional coverage
    @Test
    public void validateNoTrimOnVarchar() throws Exception {
        methodWatcher.executeUpdate("create table left1 (col1 int, col2 varchar(25))");
        methodWatcher.executeUpdate("create table right1 (col1 int, col2 varchar(25))");
        methodWatcher.executeUpdate("insert into left1 values (1,'123')");
        methodWatcher.executeUpdate("insert into right1 values (1,'123')");
        methodWatcher.executeUpdate("insert into right1 values (1,'123 ')");
        methodWatcher.executeUpdate("insert into right1 values (1,'123  ')");
        ResultSet rs = methodWatcher.executeQuery("select * from left1 left outer join right1 --splice-properties joinStrategy=NESTEDLOOP\n" +
                " on left1.col2 = right1.col2");
        Assert.assertEquals("NestedLoop Returned Extra Row",1,SpliceUnitTest.resultSetSize(rs)); //DB-4883
        rs = methodWatcher.executeQuery("select * from left1 left outer join right1 --splice-properties joinStrategy=BROADCAST\n" +
                " on left1.col2 = right1.col2");
        Assert.assertEquals("Broadcast Returned Extra Row",1,SpliceUnitTest.resultSetSize(rs)); //DB-4883
    }

    @Test
    public void testNLJInNonFlatternedScalarSubquery() throws Exception {
        /* test control path */
        String sql = "select (select max(b1) from t1,t2) as X from t1  --splice-properties useSpark=false";
        String expected = "X |\n" +
                "----\n" +
                " 3 |\n" +
                " 3 |\n" +
                " 3 |";

        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* test spark path */
        sql = "select (select max(b1) from t1,t2) as X from t1  --splice-properties useSpark=true";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testUpdateWithNLJInNonFlatternedScalarSubquery() throws Exception {
        TestConnection conn = methodWatcher.getOrCreateConnection();
        boolean oldAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);

        try (Statement s = conn.createStatement()) {
            /* update Q1 through control */
            String sql = "update t1  --splice-proeprties useSpark=false\n " +
                    "set b1=(select max(b1) from t1 inner join t2 on 1=1) where a1=1";
            int n = s.executeUpdate(sql);
            Assert.assertEquals("Incorrect number of rows updated", 1, n);

            String expected = "A1 |B1 |C1 |\n" +
                    "------------\n" +
                    " 1 | 3 | 1 |\n" +
                    " 2 | 2 | 2 |\n" +
                    " 3 | 3 | 3 |";
            ResultSet rs = methodWatcher.executeQuery("select * from t1");
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();

            /* update Q1 through spark again */
            sql = "update t1  --splice-proeprties useSpark=true\n " +
                    "set b1=(select min(b1) from t1 inner join t2 on 1=1) where a1=1";
            n = s.executeUpdate(sql);
            Assert.assertEquals("Incorrect number of rows updated", 1, n);

            expected = "A1 |B1 |C1 |\n" +
                    "------------\n" +
                    " 1 | 2 | 1 |\n" +
                    " 2 | 2 | 2 |\n" +
                    " 3 | 3 | 3 |";
            rs = methodWatcher.executeQuery("select * from t1");
            assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
        } finally {
            // roll back the update
            conn.rollback();
            conn.setAutoCommit(oldAutoCommit);
        }
    }
}