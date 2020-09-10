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
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by yxia on 1/26/20.
 */
public class FlattenedOuterJoinIT  extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(FlattenedOuterJoinIT.class);
    public static final String CLASS_NAME = FlattenedOuterJoinIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);


    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1(a1 int not null, b1 int, c1 int)")
                .withInsert("insert into t1 values(?,?,?)")
                .withRows(rows(
                        row(1, 10, 1),
                        row(2, 20, 2),
                        row(2, 20, 2),
                        row(3, 30, 3),
                        row(4, 40, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t2 (a2 int not null, b2 int, c2 int)")
                .withInsert("insert into t2 values(?,?,?)")
                .withRows(rows(
                        row(2, 20, 2),
                        row(2, 20, 2),
                        row(3, 30, 3),
                        row(4, 40, null),
                        row(5, 50, null),
                        row(6, 60, 6)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t3 (a3 int not null, b3 int, c3 int)")
                .withInsert("insert into t3 values(?,?,?)")
                .withRows(rows(
                        row(3, 30, 3),
                        row(5, 50, null),
                        row(6, 60, 6),
                        row(6, 60, 6),
                        row(7, 70, 7)))
                .create();

        /* create table with PKs */
        new TableCreator(conn)
                .withCreate("create table t4(a4 int not null, b4 int, c4 int, primary key (a4))")
                .withIndex("create index idx_t4 on t4(b4, c4)")
                .withInsert("insert into t4 values(?,?,?)")
                .withRows(rows(
                        row(1,10,1),
                        row(2,20,2),
                        row(20,20,2),
                        row(3,30,3),
                        row(4,40,null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t5 (a5 int not null, b5 int, c5 int, primary key (a5))")
                .withIndex("create index idx_t5 on t5(b5, c5)")
                .withInsert("insert into t5 values(?,?,?)")
                .withRows(rows(
                        row(2, 20, 2),
                        row(20, 20, 2),
                        row(3, 30, 3),
                        row(4,40,null),
                        row(5, 50, null),
                        row(6, 60, 6)))
                .create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testSimpleTwoTableLeftJoined() throws Exception {
        String sqlText = "select b1, a1, b2, a2, c2 from --splice-properties joinOrder=fixed\n t3, t1 left join t2 on a1=a2 where c3=c1 and c3=3";

        rowContainsQuery(new int[]{4,5,6,7,8}, "explain " + sqlText, methodWatcher,
                new String[]{"LeftOuterJoin"},
                new String[]{"TableScan[T2"},
                new String[]{"Join"},
                new String[]{"TableScan[T1"},
                new String[]{"TableScan[T3"});


        String expected = "B1 |A1 |B2 |A2 |C2 |\n" +
                "--------------------\n" +
                "30 | 3 |30 | 3 | 3 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testSimpleTwoTableRightJoined() throws Exception {
        String sqlText = "select b1, a1, b2, a2, c2 from --splice-properties joinOrder=fixed\n t3, t1 right join t2 on a1=a2 where c3=c2";
        rowContainsQuery(new int[]{4,5,6,7,8}, "explain " + sqlText, methodWatcher,
                new String[]{"LeftOuterJoin"},
                new String[]{"TableScan[T1"},
                new String[]{"Join"},
                new String[]{"TableScan[T2"},
                new String[]{"TableScan[T3"});


        String expected = "B1  | A1  |B2 |A2 |C2 |\n" +
                "------------------------\n" +
                " 30  |  3  |30 | 3 | 3 |\n" +
                "NULL |NULL |60 | 6 | 6 |\n" +
                "NULL |NULL |60 | 6 | 6 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testConsecutiveOuterJoins() throws Exception {
        String sqlText = "select * from t1 left join t2 on a1=a2 left join t3 on a1=a3 order by 1,2,3";
        String expected = "A1 |B1 | C1  | A2  | B2  | C2  | A3  | B3  | C3  |\n" +
                "--------------------------------------------------\n" +
                " 1 |10 |  1  |NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                " 3 |30 |  3  |  3  | 30  |  3  |  3  | 30  |  3  |\n" +
                " 4 |40 |NULL |  4  | 40  |NULL |NULL |NULL |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testMixtureOfInnerAndOuterJoins() throws Exception {
        String sqlText = "select a1, b1, a2, b2, a4, b4, a5, b5 from (t1 inner join t2 on a1=a2) left join (t4 left join t5 on a4=a5) on a1=a4 order by 1,2";
        String expected = "A1 |B1 |A2 |B2 |A4 |B4 |A5 |B5 |\n" +
                "--------------------------------\n" +
                " 2 |20 | 2 |20 | 2 |20 | 2 |20 |\n" +
                " 2 |20 | 2 |20 | 2 |20 | 2 |20 |\n" +
                " 2 |20 | 2 |20 | 2 |20 | 2 |20 |\n" +
                " 2 |20 | 2 |20 | 2 |20 | 2 |20 |\n" +
                " 3 |30 | 3 |30 | 3 |30 | 3 |30 |\n" +
                " 4 |40 | 4 |40 | 4 |40 | 4 |40 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testSingleTableConditionInONClause() throws Exception {
        String sqlText = "select * from t1 left join t2 on a1=a2 and a1=1 order by a1, b1";
        rowContainsQuery(new int[]{4}, "explain " + sqlText, methodWatcher,
                new String[]{"LeftOuterJoin", "preds=[(A1[4:1] = 1),(A1[4:1] = A2[4:4])]"});

        String expected = "A1 |B1 | C1  | A2  | B2  | C2  |\n" +
                "--------------------------------\n" +
                " 1 |10 |  1  |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |NULL |NULL |NULL |\n" +
                " 3 |30 |  3  |NULL |NULL |NULL |\n" +
                " 4 |40 |NULL |NULL |NULL |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testOuterJoinWithDT() throws Exception {
        String sqlText = "select a3, b3, a2, b2, a1, b1 from t1 left join (select * from t2 left join t3 on a2=a3) dt on a1=a2 and a2=1 order by a1, b1, a2, b2";
        rowContainsQuery(new int[]{5, 6, 7, 8, 9, 10}, "explain " + sqlText, methodWatcher,
                new String[]{"LeftOuterJoin", "preds=[(A1[10:1] = A2[10:3])]"},
                new String[]{"ProjectRestrict", "preds=[(A2[8:1] = 1)]"},
                new String[]{"LeftOuterJoin", "preds=[(A2[6:1] = A3[6:3])]"},
                new String[]{"TableScan[T3"},
                new String[]{"TableScan[T2"},
                new String[]{"TableScan[T1"});

        String expected = "A3  | B3  | A2  | B2  |A1 |B1 |\n" +
                "--------------------------------\n" +
                "NULL |NULL |NULL |NULL | 1 |10 |\n" +
                "NULL |NULL |NULL |NULL | 2 |20 |\n" +
                "NULL |NULL |NULL |NULL | 2 |20 |\n" +
                "NULL |NULL |NULL |NULL | 3 |30 |\n" +
                "NULL |NULL |NULL |NULL | 4 |40 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testOuterJoinWithDTAndSingleTableConditionOnOuter() throws Exception {
        String sqlText = "select a3, b3, a2, b2, a1, b1 from t1 right join (select * from t2 left join t3 on a2=a3) dt on a1=a2 and a2=3 order by a1, b1, a2, b2";
        rowContainsQuery(new int[]{5, 6, 7, 8, 9}, "explain " + sqlText, methodWatcher,
                new String[]{"LeftOuterJoin", "preds=[(A2[10:1] = 3),(A1[10:5] = A2[10:1])]"},
                new String[]{"TableScan[T1"},
                new String[]{"LeftOuterJoin", "preds=[(A2[4:1] = A3[4:3])]"},
                new String[]{"TableScan[T3"},
                new String[]{"TableScan[T2"});

        String expected = "A3  | B3  |A2 |B2 | A1  | B1  |\n" +
                "--------------------------------\n" +
                "  3  | 30  | 3 |30 |  3  | 30  |\n" +
                "NULL |NULL | 2 |20 |NULL |NULL |\n" +
                "NULL |NULL | 2 |20 |NULL |NULL |\n" +
                "NULL |NULL | 4 |40 |NULL |NULL |\n" +
                "  5  | 50  | 5 |50 |NULL |NULL |\n" +
                "  6  | 60  | 6 |60 |NULL |NULL |\n" +
                "  6  | 60  | 6 |60 |NULL |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testOuterJoinWithUnion() throws Exception {
        String sqlText = "select * from t1 left join (select a2 as X from t2 union all select a3 as X from t3) dt --splice-properties joinStrategy=nestedloop \n on a1=X and X=3";
        rowContainsQuery(new int[]{3, 4, 5, 6, 7}, "explain " + sqlText, methodWatcher,
                new String[]{"LeftOuterJoin"},
                new String[]{"ProjectRestrict", "preds=[(X[8:1] = 3)]"},
                new String[]{"Union"},
                new String[]{"TableScan[T3", "preds=[(A1[1:1] = A3[5:1])]"},
                new String[]{"TableScan[T2", "preds=[(A1[1:1] = A2[2:1])]"});

        String expected = "A1 |B1 | C1  |  X  |\n" +
                "--------------------\n" +
                " 1 |10 |  1  |NULL |\n" +
                " 2 |20 |  2  |NULL |\n" +
                " 2 |20 |  2  |NULL |\n" +
                " 3 |30 |  3  |  3  |\n" +
                " 3 |30 |  3  |  3  |\n" +
                " 4 |40 |NULL |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testOrderByElimination() throws Exception {
        /* order by left table columns */
        String sqlText = "select * from t4 left join t5 on a4=a5 order by a4";
        queryDoesNotContainString("explain " + sqlText, "OrderBy", methodWatcher);

        String expected = "A4 |B4 | C4  | A5  | B5  | C5  |\n" +
                "--------------------------------\n" +
                " 1 |10 |  1  |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |  2  | 20  |  2  |\n" +
                "20 |20 |  2  | 20  | 20  |  2  |\n" +
                " 3 |30 |  3  |  3  | 30  |  3  |\n" +
                " 4 |40 |NULL |  4  | 40  |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* order by on top of a sortmerge join */
        sqlText = "select * from t4 left join t5 --splice-properties joinStrategy=sortmerge\n on a4=a5 order by a4";
        rowContainsQuery(3, "explain " + sqlText, "OrderBy", methodWatcher);

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* order by right table columns */
        sqlText = "select * from t4 left join t5 on a4=a5 order by a5";
        rowContainsQuery(3, "explain " + sqlText, "OrderBy", methodWatcher);

        expected = "A4 |B4 | C4  | A5  | B5  | C5  |\n" +
                "--------------------------------\n" +
                " 1 |10 |  1  |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |  2  | 20  |  2  |\n" +
                "20 |20 |  2  | 20  | 20  |  2  |\n" +
                " 3 |30 |  3  |  3  | 30  |  3  |\n" +
                " 4 |40 |NULL |  4  | 40  |NULL |";
        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testPostJoinCondition1() throws Exception {
        String sqlText = "select a1, a2 from t1 left join t2 on a1=a2 where coalesce(a2,3)=3 order by a1";

        rowContainsQuery(new int[]{4, 5}, "explain " + sqlText, methodWatcher,
                new String[]{"ProjectRestrict", "preds=[(coalesce(A2[4:2], 3)  = 3)]"},
                new String[]{"LeftOuterJoin"});

        String expected = "A1 | A2  |\n" +
                "----------\n" +
                " 1 |NULL |\n" +
                " 3 |  3  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testPostJoinCondition2() throws Exception {
        String sqlText = "select * from t1 left join (select * from t2, t3 where a2=a3) dt on a1=a2 where coalesce(a2,3)=3 order by a1";

        rowContainsQuery(new int[]{4, 5}, "explain " + sqlText, methodWatcher,
                new String[]{"ProjectRestrict", "preds=[(coalesce(A2[10:4], 3)  = 3)]"},
                new String[]{"LeftOuterJoin"});

        String expected = "A1 |B1 | C1  | A2  | B2  | C2  | A3  | B3  | C3  |\n" +
                "--------------------------------------------------\n" +
                " 1 |10 |  1  |NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                " 3 |30 |  3  |  3  | 30  |  3  |  3  | 30  |  3  |\n" +
                " 4 |40 |NULL |NULL |NULL |NULL |NULL |NULL |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testPostJoinConditionAndNotExists1() throws Exception {
        String sqlText = "select * from t1 left join t2 on a1=a2 where coalesce(a2,3)=3 and not exists (select 1 from t3 where a1=a3)";

        rowContainsQuery(new int[]{4, 5, 6, 7}, "explain " + sqlText, methodWatcher,
                new String[]{"AntiJoin", "preds=[(A1[8:1] = A3[8:7])]"},
                new String[]{"TableScan[T3"},
                new String[]{"ProjectRestrict", "preds=[(coalesce(A2[4:4], 3)  = 3)]"},
                new String[]{"LeftOuterJoin", "preds=[(A1[4:1] = A2[4:4])]"});

        String expected = "A1 |B1 |C1 | A2  | B2  | C2  |\n" +
                "------------------------------\n" +
                " 1 |10 | 1 |NULL |NULL |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testPostJoinConditionAndNotExists2() throws Exception {
        String sqlText = "select * from t1 left join t2 on a1=a2 where coalesce(a2,3)=3 and not exists (select 1 from t3 where a1=a3 and a1=1)";

        rowContainsQuery(new int[]{3, 4, 8, 9}, "explain " + sqlText, methodWatcher,
                new String[]{"ProjectRestrict", "preds=[is null(subq=8)]"},
                new String[]{"Subquery"},
                new String[]{"ProjectRestrict", "preds=[(coalesce(A2[4:4], 3)  = 3)]"},
                new String[]{"LeftOuterJoin", "preds=[(A1[4:1] = A2[4:4])]"});

        String expected = "A1 |B1 |C1 | A2  | B2  | C2  |\n" +
                "------------------------------\n" +
                " 1 |10 | 1 |NULL |NULL |NULL |\n" +
                " 3 |30 | 3 |  3  | 30  |  3  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testOuterJoinInDT() throws Exception {
        String sqlText = "select a1, a2, a3 from (select * from t1 left join t2 on a1=a2 left join t3 on a1=a3) dt where a2=2";

        rowContainsQuery(new int[]{3, 4, 5, 6}, "explain " + sqlText, methodWatcher,
                new String[]{"LeftOuterJoin", "preds=[(A1[8:1] = A3[8:3])]"},
                new String[]{"TableScan[T3"},
                new String[]{"ProjectRestrict", "preds=[(A2[4:2] = 2)]"},
                new String[]{"LeftOuterJoin", "preds=[(A1[4:1] = A2[4:2])]"});

        String expected = "A1 |A2 | A3  |\n" +
                "--------------\n" +
                " 2 | 2 |NULL |\n" +
                " 2 | 2 |NULL |\n" +
                " 2 | 2 |NULL |\n" +
                " 2 | 2 |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testOuterJoinConversionAndFlattening() throws Exception {
        String sqlText = "select a1, a2, a3 from t1 full join t2 on a1=a2 full join t3 on a2=a3 where a2=3";

        rowContainsQuery(new int[]{4, 5, 6}, "explain " + sqlText, methodWatcher,
                new String[]{"LeftOuterJoin"},
                new String[]{"TableScan[T3"},
                new String[]{"LeftOuterJoin"});

        String expected = "A1 |A2 |A3 |\n" +
                "------------\n" +
                " 3 | 3 | 3 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }


    @Test
    public void testSubqueryInOnClause() throws Exception {
        String sqlText = "select * from t1 left join t2 on a1=a2 and a1 in (select a3 from t3)";

        rowContainsQuery(new int[]{3}, "explain " + sqlText, methodWatcher,
                new String[]{"LeftOuterJoin", "preds=[is not null(subq=7),(A1[8:1] = A2[8:4])]"});

        String expected = "A1 |B1 | C1  | A2  | B2  | C2  |\n" +
                "--------------------------------\n" +
                " 1 |10 |  1  |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |NULL |NULL |NULL |\n" +
                " 3 |30 |  3  |  3  | 30  |  3  |\n" +
                " 4 |40 |NULL |NULL |NULL |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testDB9593() throws Exception {
        String sqlText = "select * from t1 --splice-properties useDefaultRowCount=25000\n" +
                "left join t2 on a1=a2 and a1 in (select a3 from t3)";

        rowContainsQuery(new int[]{3}, "explain " + sqlText, methodWatcher,
                new String[]{"LeftOuterJoin", "preds=[is not null(subq=7),(A1[8:1] = A2[8:4])]"});

        String expected = "A1 |B1 | C1  | A2  | B2  | C2  |\n" +
                "--------------------------------\n" +
                " 1 |10 |  1  |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |NULL |NULL |NULL |\n" +
                " 3 |30 |  3  |  3  | 30  |  3  |\n" +
                " 4 |40 |NULL |NULL |NULL |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }
}
