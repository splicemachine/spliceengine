/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

        new TableCreator(conn)
            .withCreate("CREATE TABLE tt1 (\n" +
                "concept_id INTEGER NOT NULL PRIMARY KEY\n" +
                ")")
            .withInsert("insert into tt1 values(?)")
            .withRows(rows(
                row(1)))
            .create();

        new TableCreator(conn)
            .withCreate("CREATE TABLE tt2 (\n" +
                "v_o_id  INTEGER NOT NULL\n" +
                "GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,\n" +
                "c_s_id  INTEGER\n" +
                ")")
            .withIndex("create index tt2_v_o_id_idx on tt2(v_o_id)")
            .withInsert("insert into tt2 (c_s_id) values(?)")
            .withRows(rows(
                row(1)))
            .create();

        new TableCreator(conn)
            .withCreate("CREATE TABLE tt3 (\n" +
                "m_id    INTEGER NOT NULL\n" +
                "GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,\n" +
                "m_c_id  INTEGER NOT NULL,\n" +
                "v_c_id  INTEGER,\n" +
                "v_o_id  INTEGER\n" +
                ")")
            .withIndex("create index tt3_m_id_idx on tt3(m_id)")
            .withInsert("insert into tt3 ( m_c_id, v_c_id, v_o_id) values(?,?,?)")
            .withRows(rows(
                row(1,1,1)))
            .create();

        new TableCreator(conn)
            .withCreate("CREATE VIEW v1 (\n" +
                "m_id,\n" +
                "m_c_id,\n" +
                "v_o_id,\n" +
                "c_s_id,\n" +
                "v_c_id\n" +
                ") AS\n" +
                "SELECT m_id,\n" +
                "m_c_id,\n" +
                "v_o_id,\n" +
                "c_s_id,\n" +
                "v_c_id\n" +
                "FROM --splice-properties joinOrder=fixed\n" +
                "tt3 m --splice-properties joinStrategy=NESTEDLOOP, useSpark=true, index=tt3_m_id_idx\n" +
                "JOIN tt1 c  --splice-properties joinStrategy=NESTEDLOOP\n" +
                "ON m.m_c_id = c.concept_id\n" +
                "JOIN tt2 vo  --splice-properties joinStrategy=NESTEDLOOP, index=tt2_v_o_id_idx\n" +
                "USING (v_o_id)")
            .create();

        new TableCreator(conn)
                .withCreate("CREATE TABLE t3 (\n" +
                        "a3 varchar(6), b3 varchar(6))")
                .withInsert("insert into t3(a3,b3) values(?,?)")
                .withRows(rows(row("1", "a"),
                        row("2", "b"),
                        row("3", "c"),
                        row("4", "d"),
                        row("5", "e"),
                        row("6", "f"),
                        row("7", "g"),
                        row("8", "h"),
                        row("9", "i"),
                        row("10", "j"),
                        row("11", "k"),
                        row("12", "l")))
                .create();
        conn.commit();

        new TableCreator(conn)
                .withCreate("CREATE TABLE t4 (\n" +
                        "a4 varchar(6), b4 varchar(6))")
                .withInsert("insert into t4(a4, b4) values(?,?)")
                .withRows(rows(row("1", "a")))
                .create();
        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), schemaWatcher.toString());
    }

    /* Regression test for DB-7673 */
    @Test
    public void testIndexLookupWithNestedJoin() throws Exception {
        ResultSet rs = null;
        try {
            rs = methodWatcher.executeQuery("SELECT * FROM v1 --splice-properties useSpark=true\n" +
                "where M_ID >=1 and M_ID <=100");
        } catch (SQLException e) {
            fail("Query not expected to fail.");
        }
        assertEquals("" +
            "M_ID |M_C_ID |V_O_ID |C_S_ID |V_C_ID |\n" +
            "--------------------------------------\n" +
            "  1  |   1   |   1   |   1   |   1   |", toString(rs));
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

    @Test
    public void testNLJWithUnionAndTopN() throws Exception {
        /* test control path */
        String sql = "select b3\n" +
                "        FROM --splice-properties joinOrder=fixed\n" +
                "        t3 --splice-properties useSpark=false\n" +
                "        , (SELECT *\n" +
                "        FROM (select top 1 * from t4) S\n" +
                "        UNION\n" +
                "        SELECT *\n" +
                "        FROM (select  top 1 * from t4) S) V --splice-properties joinStrategy=nestedloop\n" +
                "        WHERE t3.a3 = V.a4 AND b3 LIKE '%'";
        String expected = "B3 |\n" +
                "----\n" +
                " a |";

        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* check that plan should not push down the predicate under top N */
        /* the plan should look like the following: the predicate of t3.A3=V.A4 should not be applied on TableScan(n=3,5) of T4
           due to the limit operation, but on the ProjectRestrict step (n=9)
        Plan
        ---------------------------------------------------------------------------------------------------
        Cursor(n=13,rows=400,updateMode=READ_ONLY (1),engine=control)
          ->  ScrollInsensitive(n=12,totalCost=177.8,outputRows=400,outputHeapSize=400 B,partitions=1)
            ->  ProjectRestrict(n=11,totalCost=169.4,outputRows=400,outputHeapSize=400 B,partitions=1)
              ->  NestedLoopJoin(n=10,totalCost=169.4,outputRows=400,outputHeapSize=400 B,partitions=1)
                ->  ProjectRestrict(n=9,totalCost=169.4,outputRows=400,outputHeapSize=400 B,partitions=1,preds=[(T3.A3[1:1] = V.A4[16:1])])
                  ->  Distinct(n=8,totalCost=169.4,outputRows=400,outputHeapSize=400 B,partitions=1)
                    ->  Union(n=7,totalCost=169.4,outputRows=400,outputHeapSize=400 B,partitions=1)
                      ->  Limit(n=6,totalCost=0.212,outputRows=1,outputHeapSize=2 B,partitions=1,fetchFirst=1)
                        ->  TableScan[T4(44240)](n=5,totalCost=0.202,scannedRows=1,outputRows=1,outputHeapSize=2 B,partitions=1)
                      ->  Limit(n=4,totalCost=0.212,outputRows=1,outputHeapSize=2 B,partitions=1,fetchFirst=1)
                        ->  TableScan[T4(44240)](n=3,totalCost=0.202,scannedRows=1,outputRows=1,outputHeapSize=2 B,partitions=1)
                ->  ProjectRestrict(n=2,totalCost=4.04,outputRows=10,outputHeapSize=20 B,partitions=1,preds=[like(B3[0:2], %)])
                  ->  TableScan[T3(44224)](n=1,totalCost=4.04,scannedRows=20,outputRows=20,outputHeapSize=20 B,partitions=1)
        13 rows selected
         */
        rs = methodWatcher.executeQuery("explain " + sql);
        int level = 5;
        for(int i=0;i<level;i++){
            rs.next();
        }
        String actualString=rs.getString(1);
        String failMessage=String.format("expected result of query '%s' to contain '%s' at row %,d but did not, actual result was '%s'",
                sql,"ProjectRestrict and preds",level,actualString);
        Assert.assertTrue(failMessage,actualString.contains("ProjectRestrict") && actualString.contains("preds"));


        /* test spark path */
        sql = "select b3\n" +
                "        FROM --splice-properties joinOrder=fixed\n" +
                "        t3 --splice-properties useSpark=true\n" +
                "        , (SELECT *\n" +
                "        FROM (select top 1 * from t4) S\n" +
                "        UNION\n" +
                "        SELECT *\n" +
                "        FROM (select  top 1 * from t4) S) V --splice-properties joinStrategy=nestedloop\n" +
                "        WHERE t3.a3 = V.a4 AND b3 LIKE '%'";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testNLJWithUnion() throws Exception {
        /* test control path */
        String sql = "select b3\n" +
                "        FROM --splice-properties joinOrder=fixed\n" +
                "        t3 --splice-properties useSpark=false\n" +
                "        , (SELECT *\n" +
                "        FROM (select * from t4) S\n" +
                "        UNION\n" +
                "        SELECT *\n" +
                "        FROM (select * from t4) S) V --splice-properties joinStrategy=nestedloop\n" +
                "        WHERE t3.a3 = V.a4 AND b3 LIKE '%'";
        String expected = "B3 |\n" +
                "----\n" +
                " a |";

        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* test spark path */
        sql = "select b3\n" +
                "        FROM --splice-properties joinOrder=fixed\n" +
                "        t3 --splice-properties useSpark=true\n" +
                "        , (SELECT *\n" +
                "        FROM (select * from t4) S\n" +
                "        UNION\n" +
                "        SELECT *\n" +
                "        FROM (select * from t4) S) V --splice-properties joinStrategy=nestedloop\n" +
                "        WHERE t3.a3 = V.a4 AND b3 LIKE '%'";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testNLJWithRightTableContainsSubquery() throws Exception {
        /* test control path */
        String sql = "select b3\n" +
                "        FROM --splice-properties joinOrder=fixed\n" +
                "        t3 --splice-properties useSpark=false\n" +
                "        , (SELECT *\n" +
                "        FROM (select * from t4 where b4 in (values 'a', 'd')) S) V --splice-properties joinStrategy=nestedloop\n" +
                "        WHERE t3.a3 = V.a4 AND b3 LIKE '%'";
        String expected = "B3 |\n" +
                "----\n" +
                " a |";

        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* test spark path */
        sql = "select b3\n" +
                "        FROM --splice-properties joinOrder=fixed\n" +
                "        t3 --splice-properties useSpark=true\n" +
                "        , (SELECT *\n" +
                "        FROM (select * from t4 where b4 in (values 'a', 'd')) S) V --splice-properties joinStrategy=nestedloop\n" +
                "        WHERE t3.a3 = V.a4 AND b3 LIKE '%'";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testNLJWithRightTableContainsUnionAndJoin() throws Exception {
                /* test control path */
        String sql = "select b3\n" +
                "        FROM --splice-properties joinOrder=fixed\n" +
                "        t3 --splice-properties useSpark=false\n" +
                "        , (SELECT *\n" +
                "        FROM (select * from t4) S\n" +
                "        UNION\n" +
                "        SELECT *\n" +
                "        FROM (select X.* from --splice-properties joinOrder=fixed\n" +
                "t4 as Y, t4 as X --splice-properties joinStrategy=broadcast\n" +
                " where X.a4=Y.a4) S) V --splice-properties joinStrategy=nestedloop\n" +
                "        WHERE t3.a3 = V.a4 AND b3 LIKE '%'";
        String expected = "B3 |\n" +
                "----\n" +
                " a |";

        /* plan should looks like below where T3.A3[1:1] = S.A4[7:1] is pushed down
        Plan
        ----
        Cursor(n=21,rows=182,updateMode=READ_ONLY (1),engine=OLTP (query hint))
          ->  ScrollInsensitive(n=21,totalCost=1144.672,outputRows=182,outputHeapSize=964 B,partitions=1)
            ->  ProjectRestrict(n=20,totalCost=981.824,outputRows=182,outputHeapSize=964 B,partitions=1)
              ->  NestedLoopJoin(n=18,totalCost=981.824,outputRows=182,outputHeapSize=964 B,partitions=1)
                ->  ProjectRestrict(n=17,totalCost=981.824,outputRows=182,outputHeapSize=964 B,partitions=1)
                  ->  Distinct(n=16,totalCost=981.824,outputRows=182,outputHeapSize=964 B,partitions=1)
                    ->  Union(n=14,totalCost=16.336,outputRows=18,outputHeapSize=60 B,partitions=2)
                      ->  ProjectRestrict(n=11,totalCost=12.296,outputRows=16,outputHeapSize=56 B,partitions=1)
                        ->  BroadcastJoin(n=9,totalCost=12.296,outputRows=16,outputHeapSize=56 B,partitions=1,preds=[(X.A4[9:2] = Y.A4[9:1])])
                          ->  TableScan[T4(1712)](n=7,totalCost=4.04,scannedRows=20,outputRows=20,outputHeapSize=56 B,partitions=1,preds=[(T3.A3[1:1] = X.A4[7:1])])
                          ->  TableScan[T4(1712)](n=5,totalCost=4.04,scannedRows=20,outputRows=20,outputHeapSize=20 B,partitions=1)
                      ->  TableScan[T4(1712)](n=2,totalCost=4.04,scannedRows=20,outputRows=2,outputHeapSize=4 B,partitions=1,preds=[(T3.A3[1:1] = T4.A4[2:1])])
                ->  ProjectRestrict(n=1,totalCost=4.04,outputRows=10,outputHeapSize=20 B,partitions=1,preds=[like(B3[0:2], %) ])
                  ->  TableScan[T3(1696)](n=0,totalCost=4.04,scannedRows=20,outputRows=20,outputHeapSize=20 B,partitions=1)

         */
        rowContainsQuery(new int[]{9, 10, 11, 12}, "explain " + sql, methodWatcher,
                new String[]{"BroadcastJoin", "preds=[(X.A4[9:2] = Y.A4[9:1])]"},
                new String[]{"TableScan[T4", "preds=[(T3.A3[1:1] = X.A4[7:1])]"},
                new String[]{"TableScan[T4"},
                new String[]{"TableScan[T4", "preds=[(T3.A3[1:1] = T4.A4[2:1])]"});

        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* test spark path */
        sql = "select b3\n" +
                "        FROM --splice-properties joinOrder=fixed\n" +
                "        t3 --splice-properties useSpark=true\n" +
                "        , (SELECT *\n" +
                "        FROM (select * from t4) S\n" +
                "        UNION\n" +
                "        SELECT *\n" +
                "        FROM (select X.* from t4 as X, t4 as Y where X.a4=Y.a4) S) V --splice-properties joinStrategy=nestedloop\n" +
                "        WHERE t3.a3 = V.a4 AND b3 LIKE '%'";
        rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testNLJWithValuesOnTheLeft() throws Exception {
                /* test control path */
        String sql = "select * from (values (1,5)) ctx(c1,c3) inner join t1 --splice-properties useDefaultRowCount=3000\n" +
                "on ctx.c1=t1.a1";
        String expected = "C1 |C3 |A1 |B1 |C1 |\n" +
                "--------------------\n" +
                " 1 | 5 | 1 | 1 | 1 |";

        /* check the plan using nestedloop join with values on the left
        Plan
        ----
        Cursor(n=7,rows=300,updateMode=READ_ONLY (1),engine=OLTP (default))
          ->  ScrollInsensitive(n=7,totalCost=18.916,outputRows=300,outputHeapSize=900 B,partitions=1)
            ->  NestedLoopJoin(n=4,totalCost=11.908,outputRows=300,outputHeapSize=900 B,partitions=1)
              ->  TableScan[T1(1888)](n=2,totalCost=4.6,scannedRows=300,outputRows=300,outputHeapSize=900 B,partitions=1,preds=[(CTX.SQLCol1[1:1] = T1.A1[2:1])])
              ->  Values(n=0,totalCost=0,outputRows=1,outputHeapSize=0 B,partitions=1)
         */

        rowContainsQuery(new int[]{3, 4, 5}, "explain " + sql, methodWatcher,
                new String[]{"NestedLoopJoin"},
                new String[]{"TableScan[T1", "keys=[(CTX.SQLCol1[1:1] = T1.A1[2:1])]"},
                new String[]{"Values", "outputRows=1"});

        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals("\n" + sql + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

    }
}
