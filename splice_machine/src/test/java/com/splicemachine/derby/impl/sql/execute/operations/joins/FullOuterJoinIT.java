/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
import com.splicemachine.subquery.SubqueryITUtil;
import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.spark_project.guava.collect.Lists;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.splicemachine.subquery.SubqueryITUtil.ONE_SUBQUERY_NODE;
import static com.splicemachine.subquery.SubqueryITUtil.TWO_SUBQUERY_NODES;
import static com.splicemachine.subquery.SubqueryITUtil.ZERO_SUBQUERY_NODES;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by yxia on 12/2/19.
 */
@RunWith(Parameterized.class)
public class FullOuterJoinIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(FullOuterJoinIT.class);
    public static final String CLASS_NAME = FullOuterJoinIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(4);
        params.add(new Object[]{"true", "sortmerge"});
        params.add(new Object[]{"true", "broadcast"});
        params.add(new Object[]{"false", "sortmerge"});
        params.add(new Object[]{"false", "broadcast"});
        return params;
    }

    private String useSpark;
    private String joinStrategy;

    public FullOuterJoinIT(String useSpark, String joinStrategy) {
        this.useSpark = useSpark;
        this.joinStrategy = joinStrategy;
    }

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
                        row(1,10,1),
                        row(2,20,2),
                        row(2,20,2),
                        row(3,30,3),
                        row(4,40,null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t11(a1 int not null, b1 int, c1 int)")
                .withIndex("create index idx_t11 on t11(b1)")
                .withInsert("insert into t11 values(?,?,?)")
                .withRows(rows(
                        row(1,10,1),
                        row(2,20,2),
                        row(2,20,2),
                        row(3,30,3),
                        row(4,40,null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t2 (a2 int not null, b2 int, c2 int)")
                .withInsert("insert into t2 values(?,?,?)")
                .withRows(rows(
                        row(2, 20, 2),
                        row(2, 20, 2),
                        row(3, 30, 3),
                        row(4,40,null),
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
    public void simpleTwoTableFullJoined() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on c1=c2", useSpark, joinStrategy);
        String expected = "B1  | A1  | B2  | A2  | C2  |\n" +
                "------------------------------\n" +
                " 10  |  1  |NULL |NULL |NULL |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 30  |  3  | 30  |  3  |  3  |\n" +
                " 40  |  4  |NULL |NULL |NULL |\n" +
                "NULL |NULL | 40  |  4  |NULL |\n" +
                "NULL |NULL | 50  |  5  |NULL |\n" +
                "NULL |NULL | 60  |  6  |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void twoTableFullJoinedWithBothEqualityAndNonEqualityCondition() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on c1+1=c2 and a1<a2", useSpark, joinStrategy);
        String expected = "B1  | A1  | B2  | A2  | C2  |\n" +
                "------------------------------\n" +
                " 10  |  1  | 20  |  2  |  2  |\n" +
                " 10  |  1  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 30  |  3  |  3  |\n" +
                " 20  |  2  | 30  |  3  |  3  |\n" +
                " 30  |  3  |NULL |NULL |NULL |\n" +
                " 40  |  4  |NULL |NULL |NULL |\n" +
                "NULL |NULL | 40  |  4  |NULL |\n" +
                "NULL |NULL | 50  |  5  |NULL |\n" +
                "NULL |NULL | 60  |  6  |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void twoTableFullJoinedThroughRDDImplementation() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1=a2 and case when c1=2 then 2 end=c2", useSpark, joinStrategy);
        String expected = "B1  | A1  | B2  | A2  | C2  |\n" +
                "------------------------------\n" +
                " 10  |  1  |NULL |NULL |NULL |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 30  |  3  |NULL |NULL |NULL |\n" +
                " 40  |  4  |NULL |NULL |NULL |\n" +
                "NULL |NULL | 30  |  3  |  3  |\n" +
                "NULL |NULL | 40  |  4  |NULL |\n" +
                "NULL |NULL | 50  |  5  |NULL |\n" +
                "NULL |NULL | 60  |  6  |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void fullJoinWithATableWithSingleTableCondition() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join (select * from t2 --splice-properties useSpark=%s\n " +
                "where a2=3) dt --splice-properties joinStrategy=%s\n on a1=a2", useSpark, joinStrategy);
        String expected = "B1 |A1 | B2  | A2  | C2  |\n" +
                "--------------------------\n" +
                "10 | 1 |NULL |NULL |NULL |\n" +
                "20 | 2 |NULL |NULL |NULL |\n" +
                "20 | 2 |NULL |NULL |NULL |\n" +
                "30 | 3 | 30  |  3  |  3  |\n" +
                "40 | 4 |NULL |NULL |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void fullJoinWithInEqualityCondition() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1>a2", useSpark, joinStrategy);
        String expected = "B1  | A1  | B2  | A2  | C2  |\n" +
                "------------------------------\n" +
                " 10  |  1  |NULL |NULL |NULL |\n" +
                " 20  |  2  |NULL |NULL |NULL |\n" +
                " 20  |  2  |NULL |NULL |NULL |\n" +
                " 30  |  3  | 20  |  2  |  2  |\n" +
                " 30  |  3  | 20  |  2  |  2  |\n" +
                " 40  |  4  | 20  |  2  |  2  |\n" +
                " 40  |  4  | 20  |  2  |  2  |\n" +
                " 40  |  4  | 30  |  3  |  3  |\n" +
                "NULL |NULL | 40  |  4  |NULL |\n" +
                "NULL |NULL | 50  |  5  |NULL |\n" +
                "NULL |NULL | 60  |  6  |  6  |";

        try {
            ResultSet rs = methodWatcher.executeQuery(sqlText);
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
        }catch (SQLException e) {
            if (joinStrategy.equals("sortmerge"))
                Assert.assertTrue("Invalid exception thrown: " + e, e.getMessage().startsWith("No valid execution plan"));
            else
                Assert.fail("Unexpected exception: " + e.getMessage() + "... JoinStrategy=" + joinStrategy + ", useSpark=" + useSpark);
        }
    }

    @Test
    public void fullJoinWithInEqualityConditionThroughRDDImplementation() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1>a2 and case when c1=3 then 3 end>c2", useSpark, joinStrategy);
        String expected = "B1  | A1  | B2  | A2  | C2  |\n" +
                "------------------------------\n" +
                " 10  |  1  |NULL |NULL |NULL |\n" +
                " 20  |  2  |NULL |NULL |NULL |\n" +
                " 20  |  2  |NULL |NULL |NULL |\n" +
                " 30  |  3  | 20  |  2  |  2  |\n" +
                " 30  |  3  | 20  |  2  |  2  |\n" +
                " 40  |  4  |NULL |NULL |NULL |\n" +
                "NULL |NULL | 30  |  3  |  3  |\n" +
                "NULL |NULL | 40  |  4  |NULL |\n" +
                "NULL |NULL | 50  |  5  |NULL |\n" +
                "NULL |NULL | 60  |  6  |  6  |";

        try {
            ResultSet rs = methodWatcher.executeQuery(sqlText);
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
        } catch (SQLException e) {
            if (joinStrategy.equals("sortmerge"))
                Assert.assertTrue("Invalid exception thrown: " + e, e.getMessage().startsWith("No valid execution plan"));
            else
                Assert.fail("Unexpected exception: " + e.getMessage() + "... JoinStrategy=" + joinStrategy + ", useSpark=" + useSpark);
        }
    }

    @Test
    public void testConversionOfFullJoinWithNoConstraintsToInnerJoin() throws Exception {
        String sqlText = format("explain select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s\n on 3=3 \n" +
                "full join t3 on 1=1", useSpark);
        queryDoesNotContainString(sqlText, "Full", methodWatcher);
    }

    @Test
    public void testConversionOfFullJoinWithNoConstraintsToInnerJoin2() throws Exception {
        String sqlText = format("select * from t1 inner join t3 on a1=a3 full join t2 --splice-properties useSpark=%s\n on 3=3", useSpark);
        String expected = "A1 |B1 |C1 |A3 |B3 |C3 |A2 |B2 | C2  |\n" +
                "--------------------------------------\n" +
                " 3 |30 | 3 | 3 |30 | 3 | 2 |20 |  2  |\n" +
                " 3 |30 | 3 | 3 |30 | 3 | 2 |20 |  2  |\n" +
                " 3 |30 | 3 | 3 |30 | 3 | 3 |30 |  3  |\n" +
                " 3 |30 | 3 | 3 |30 | 3 | 4 |40 |NULL |\n" +
                " 3 |30 | 3 | 3 |30 | 3 | 5 |50 |NULL |\n" +
                " 3 |30 | 3 | 3 |30 | 3 | 6 |60 |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testOnClauseWithSingleTableCondition1() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1=a2 and b2=20", useSpark, joinStrategy);
        String expected = "B1  | A1  | B2  | A2  | C2  |\n" +
                "------------------------------\n" +
                " 10  |  1  |NULL |NULL |NULL |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 20  |  2  | 20  |  2  |  2  |\n" +
                " 30  |  3  |NULL |NULL |NULL |\n" +
                " 40  |  4  |NULL |NULL |NULL |\n" +
                "NULL |NULL | 30  |  3  |  3  |\n" +
                "NULL |NULL | 40  |  4  |NULL |\n" +
                "NULL |NULL | 50  |  5  |NULL |\n" +
                "NULL |NULL | 60  |  6  |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testOnClauseWithSingleTableCondition2() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1=a2 and b1=30", useSpark, joinStrategy);
        String expected = "B1  | A1  | B2  | A2  | C2  |\n" +
                "------------------------------\n" +
                " 10  |  1  |NULL |NULL |NULL |\n" +
                " 20  |  2  |NULL |NULL |NULL |\n" +
                " 20  |  2  |NULL |NULL |NULL |\n" +
                " 30  |  3  | 30  |  3  |  3  |\n" +
                " 40  |  4  |NULL |NULL |NULL |\n" +
                "NULL |NULL | 20  |  2  |  2  |\n" +
                "NULL |NULL | 20  |  2  |  2  |\n" +
                "NULL |NULL | 40  |  4  |NULL |\n" +
                "NULL |NULL | 50  |  5  |NULL |\n" +
                "NULL |NULL | 60  |  6  |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testOnClauseWithSingleTableCondition3() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 --splice-properties useSpark=%s\n" +
                " full join (select * from t2 where b2=30) dt --splice-properties joinStrategy=%s\n on a1=a2", useSpark, joinStrategy);
        String expected = "B1 |A1 | B2  | A2  | C2  |\n" +
                "--------------------------\n" +
                "10 | 1 |NULL |NULL |NULL |\n" +
                "20 | 2 |NULL |NULL |NULL |\n" +
                "20 | 2 |NULL |NULL |NULL |\n" +
                "30 | 3 | 30  |  3  |  3  |\n" +
                "40 | 4 |NULL |NULL |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testOnClauseWithSingleTableCondition4() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from (select * from t1 --splice-properties useSpark=%s\n where b1=30) dt " +
                " full join t2 --splice-properties joinStrategy=%s\n on a1=a2", useSpark, joinStrategy);
        String expected = "B1  | A1  |B2 |A2 | C2  |\n" +
                "--------------------------\n" +
                " 30  |  3  |30 | 3 |  3  |\n" +
                "NULL |NULL |20 | 2 |  2  |\n" +
                "NULL |NULL |20 | 2 |  2  |\n" +
                "NULL |NULL |40 | 4 |NULL |\n" +
                "NULL |NULL |50 | 5 |NULL |\n" +
                "NULL |NULL |60 | 6 |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testOnClauseWithSingleTableCondition5() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 --splice-properties useSpark=%s\n" +
                " full join (select * from t2 where b2=30) dt --splice-properties joinStrategy=%s\n on a1=a2 and c2=4", useSpark, joinStrategy);
        String expected = "B1  | A1  | B2  | A2  | C2  |\n" +
                "------------------------------\n" +
                " 10  |  1  |NULL |NULL |NULL |\n" +
                " 20  |  2  |NULL |NULL |NULL |\n" +
                " 20  |  2  |NULL |NULL |NULL |\n" +
                " 30  |  3  |NULL |NULL |NULL |\n" +
                " 40  |  4  |NULL |NULL |NULL |\n" +
                "NULL |NULL | 30  |  3  |  3  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testWhereClauseWithSingleTableCondition1() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1=a2 where b1=30", useSpark, joinStrategy);
        String expected = "B1 |A1 |B2 |A2 |C2 |\n" +
                "--------------------\n" +
                "30 | 3 |30 | 3 | 3 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testWhereClauseWithSingleTableCondition2() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1=a2 where b2>30", useSpark, joinStrategy);
        String expected = "B1  | A1  |B2 |A2 | C2  |\n" +
                "--------------------------\n" +
                " 40  |  4  |40 | 4 |NULL |\n" +
                "NULL |NULL |50 | 5 |NULL |\n" +
                "NULL |NULL |60 | 6 |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testConsecutiveFullOuterJoins() throws Exception {
        String sqlText = format("select * from t1 full join t2 --splice-properties useSpark=%s\n" +
                "full join t3 --splice-properties joinStrategy=%s\n " +
                "on a2=a3 on a1=a3", useSpark, joinStrategy);
        String expected = "A1  | B1  | C1  | A2  | B2  | C2  | A3  | B3  | C3  |\n" +
                "------------------------------------------------------\n" +
                "  1  | 10  |  1  |NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                "  2  | 20  |  2  |NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                "  2  | 20  |  2  |NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                "  3  | 30  |  3  |  3  | 30  |  3  |  3  | 30  |  3  |\n" +
                "  4  | 40  |NULL |NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |  4  | 40  |NULL |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |  5  | 50  |NULL |  5  | 50  |NULL |\n" +
                "NULL |NULL |NULL |  6  | 60  |  6  |  6  | 60  |  6  |\n" +
                "NULL |NULL |NULL |  6  | 60  |  6  |  6  | 60  |  6  |\n" +
                "NULL |NULL |NULL |NULL |NULL |NULL |  7  | 70  |  7  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testMixtureOfOuterJoins() throws Exception {
        String sqlText = format("select * from t1 left join t2 --splice-properties useSpark=%s\n" +
                "full join t3 --splice-properties joinStrategy=%s\n " +
                "on a2=a3 on a1=a3", useSpark, joinStrategy);
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

        sqlText = format("select * from t1 left join t2 --splice-properties useSpark=%s\n" +
                "full join t3 --splice-properties joinStrategy=%s\n " +
                "on a2=a3 on a1=a2", useSpark, joinStrategy);
        expected = "A1 |B1 | C1  | A2  | B2  | C2  | A3  | B3  | C3  |\n" +
                "--------------------------------------------------\n" +
                " 1 |10 |  1  |NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                " 2 |20 |  2  |  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                " 3 |30 |  3  |  3  | 30  |  3  |  3  | 30  |  3  |\n" +
                " 4 |40 |NULL |  4  | 40  |NULL |NULL |NULL |NULL |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testMixtureOfOuterJoins2() throws Exception {
        String sqlText = format("select * from t1 full join t2 --splice-properties useSpark=%s\n" +
                "left join t3 --splice-properties joinStrategy=%s\n " +
                "on a2=a3 on a1=a3", useSpark, joinStrategy);
        String expected = "A1  | B1  | C1  | A2  | B2  | C2  | A3  | B3  | C3  |\n" +
                "------------------------------------------------------\n" +
                "  1  | 10  |  1  |NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                "  2  | 20  |  2  |NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                "  2  | 20  |  2  |NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                "  3  | 30  |  3  |  3  | 30  |  3  |  3  | 30  |  3  |\n" +
                "  4  | 40  |NULL |NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |  4  | 40  |NULL |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |  5  | 50  |NULL |  5  | 50  |NULL |\n" +
                "NULL |NULL |NULL |  6  | 60  |  6  |  6  | 60  |  6  |\n" +
                "NULL |NULL |NULL |  6  | 60  |  6  |  6  | 60  |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sqlText = format("select * from t1 full join t2 --splice-properties useSpark=%s\n" +
                "left join t3 --splice-properties joinStrategy=%s\n " +
                "on a2=a3 on a1=a2", useSpark, joinStrategy);
        expected = "A1  | B1  | C1  | A2  | B2  | C2  | A3  | B3  | C3  |\n" +
                "------------------------------------------------------\n" +
                "  1  | 10  |  1  |NULL |NULL |NULL |NULL |NULL |NULL |\n" +
                "  2  | 20  |  2  |  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                "  2  | 20  |  2  |  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                "  2  | 20  |  2  |  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                "  2  | 20  |  2  |  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                "  3  | 30  |  3  |  3  | 30  |  3  |  3  | 30  |  3  |\n" +
                "  4  | 40  |NULL |  4  | 40  |NULL |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |  5  | 50  |NULL |  5  | 50  |NULL |\n" +
                "NULL |NULL |NULL |  6  | 60  |  6  |  6  | 60  |  6  |\n" +
                "NULL |NULL |NULL |  6  | 60  |  6  |  6  | 60  |  6  |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testCorrelatedSubqueryWithFullJoinInWhereClause() throws Exception {
        String sqlText = format("select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1=a2 where a1 in (select a3 from t3 where b2=b3)", useSpark, joinStrategy);
        String expected = "B1 |A1 |B2 |A2 |C2 |\n" +
                "--------------------\n" +
                "30 | 3 |30 | 3 | 3 |";

        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sqlText, ZERO_SUBQUERY_NODES, expected);
    }

    @Test
    public void testCorrelatedSSQWithFullJoin() throws Exception {
        String sqlText = format("select (select a5 from t5 where a1=a5) as ssq1, (select a5 from t5 where a2=a5) as ssq2, t1.*, t2.*  from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1=a2", useSpark, joinStrategy);
        String expected = "SSQ1 |SSQ2 | A1  | B1  | C1  | A2  | B2  | C2  |\n" +
                "------------------------------------------------\n" +
                "  2  |  2  |  2  | 20  |  2  |  2  | 20  |  2  |\n" +
                "  2  |  2  |  2  | 20  |  2  |  2  | 20  |  2  |\n" +
                "  2  |  2  |  2  | 20  |  2  |  2  | 20  |  2  |\n" +
                "  2  |  2  |  2  | 20  |  2  |  2  | 20  |  2  |\n" +
                "  3  |  3  |  3  | 30  |  3  |  3  | 30  |  3  |\n" +
                "  4  |  4  |  4  | 40  |NULL |  4  | 40  |NULL |\n" +
                "NULL |  5  |NULL |NULL |NULL |  5  | 50  |NULL |\n" +
                "NULL |  6  |NULL |NULL |NULL |  6  | 60  |  6  |\n" +
                "NULL |NULL |  1  | 10  |  1  |NULL |NULL |NULL |";

        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sqlText, TWO_SUBQUERY_NODES, expected);
    }

    @Test
    public void testFullJoinInSubquery1() throws Exception {
        String sqlText = format("select * from t11 where c1 in (select c1 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1=a2 where t11.b1=t1.b1)", useSpark, joinStrategy);
        String expected = "A1 |B1 |C1 |\n" +
                "------------\n" +
                " 1 |10 | 1 |\n" +
                " 2 |20 | 2 |\n" +
                " 2 |20 | 2 |\n" +
                " 3 |30 | 3 |";

        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sqlText, ONE_SUBQUERY_NODE, expected);
    }

    @Test
    public void testFullJoinInSubquery2() throws Exception {
        String sqlText = format("select * from t5 where c5 not in (select c1 from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1=a2 where t5.b5=t1.b1)", useSpark, joinStrategy);
        String expected = "A5 |B5 | C5  |\n" +
                "--------------\n" +
                " 5 |50 |NULL |\n" +
                " 6 |60 |  6  |";

        SubqueryITUtil.assertUnorderedResult(methodWatcher.getOrCreateConnection(), sqlText, ONE_SUBQUERY_NODE, expected);
    }


    @Test
    public void testFullJoinInDerivedTable() throws Exception {
        String sqlText = format("select * from t11, (select * from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n " +
                "on a1=a2) dt where t11.a1 = dt.a2", useSpark, joinStrategy);
        String expected = "A1 |B1 | C1  |A1 |B1 | C1  |A2 |B2 | C2  |\n" +
                "------------------------------------------\n" +
                " 2 |20 |  2  | 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 2 |20 |  2  | 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 2 |20 |  2  | 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 2 |20 |  2  | 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 2 |20 |  2  | 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 2 |20 |  2  | 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 2 |20 |  2  | 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 2 |20 |  2  | 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 3 |30 |  3  | 3 |30 |  3  | 3 |30 |  3  |\n" +
                " 4 |40 |NULL | 4 |40 |NULL | 4 |40 |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testFullJoinWithClause() throws Exception {
        String sqlText = format("with dt as select * from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1=a2\n" +
                "select * from t11, dt where t11.a1 = dt.a2", useSpark, joinStrategy);
        String expected = "A1 |B1 | C1  |A1 |B1 | C1  |A2 |B2 | C2  |\n" +
                "------------------------------------------\n" +
                " 2 |20 |  2  | 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 2 |20 |  2  | 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 2 |20 |  2  | 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 2 |20 |  2  | 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 2 |20 |  2  | 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 2 |20 |  2  | 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 2 |20 |  2  | 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 2 |20 |  2  | 2 |20 |  2  | 2 |20 |  2  |\n" +
                " 3 |30 |  3  | 3 |30 |  3  | 3 |30 |  3  |\n" +
                " 4 |40 |NULL | 4 |40 |NULL | 4 |40 |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testAggregationOnTopOfFullJoin() throws Exception {
        String sqlText = format("with dt as select a2, count(*) as CC from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1=a2 group by a2 \n" +
                "select * from t11, dt where t11.a1 = dt.a2", useSpark, joinStrategy);
        String expected = "A1 |B1 | C1  |A2 |CC |\n" +
                "----------------------\n" +
                " 2 |20 |  2  | 2 | 4 |\n" +
                " 2 |20 |  2  | 2 | 4 |\n" +
                " 3 |30 |  3  | 3 | 1 |\n" +
                " 4 |40 |NULL | 4 | 1 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    @Ignore("Wrong result for control path tracked in DB-8976")
    public void testWindowFunctionOnTopOfFullJoin() throws Exception {
        String sqlText = format("select case when a1 <=3 then 'A' else 'B' end as pid, t1.*, t2.*, max(c2) over (partition by case when a1 <=3 then 'A' else 'B' end order by a1, a2 rows between unbounded preceding and current row) as cmax from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on a1>a2 order by pid, a1, a2", useSpark, joinStrategy);
        String expected = "PID | A1  | B1  | C1  | A2  | B2  | C2  |CMAX |\n" +
                "------------------------------------------------\n" +
                "  A  |  1  | 10  |  1  |NULL |NULL |NULL |NULL |\n" +
                "  A  |  2  | 20  |  2  |NULL |NULL |NULL |NULL |\n" +
                "  A  |  2  | 20  |  2  |NULL |NULL |NULL |NULL |\n" +
                "  A  |  3  | 30  |  3  |  2  | 20  |  2  |  2  |\n" +
                "  A  |  3  | 30  |  3  |  2  | 20  |  2  |  2  |\n" +
                "  B  |  4  | 40  |NULL |  2  | 20  |  2  |  2  |\n" +
                "  B  |  4  | 40  |NULL |  2  | 20  |  2  |  2  |\n" +
                "  B  |  4  | 40  |NULL |  3  | 30  |  3  |  3  |\n" +
                "  B  |NULL |NULL |NULL |  4  | 40  |NULL |  3  |\n" +
                "  B  |NULL |NULL |NULL |  5  | 50  |NULL |  3  |\n" +
                "  B  |NULL |NULL |NULL |  6  | 60  |  6  |  6  |";

        try {
            ResultSet rs = methodWatcher.executeQuery(sqlText);
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();
        }catch (SQLException e) {
            if (joinStrategy.equals("sortmerge"))
                Assert.assertTrue("Invalid exception thrown: " + e, e.getMessage().startsWith("No valid execution plan"));
            else
                Assert.fail("Unexpected exception: " + e.getMessage() + "... JoinStrategy=" + joinStrategy + ", useSpark=" + useSpark);
        }
    }

    @Test
    public void testFullJoinInUnionAll() throws Exception {
        // predicate outside union-all cannot be pushed inside full outer join
        String sqlText = format("select * from (select a1 as X from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n" +
                "on a1=a2\n" +
                "union all\n" +
                "select a5 as X from t5) dt where X in (1,3,5,7)", useSpark, joinStrategy);

        rowContainsQuery(new int[]{5, 7, 8}, "explain " + sqlText, methodWatcher,
                new String[]{"MultiProbeTableScan", "preds=[(A5[7:1] IN (1,3,5,7))]"},
                new String[]{"ProjectRestrict", "preds=[(A1[4:1] IN (1,3,5,7))]"},
                new String[]{"FullOuterJoin", "preds=[(A1[4:1] = A2[4:2])]"});

        String expected = "X |\n" +
                "----\n" +
                " 1 |\n" +
                " 3 |\n" +
                " 3 |\n" +
                " 5 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testFullJoinWithUsing() throws Exception {
        String sqlText = format("select * from t1 full join t11 --splice-properties useSpark=%s, joinStrategy=%s\n using (b1,c1)", useSpark, joinStrategy);
        String expected = "B1  | C1  | A1  | A1  |\n" +
                "------------------------\n" +
                " 10  |  1  |  1  |  1  |\n" +
                " 20  |  2  |  2  |  2  |\n" +
                " 20  |  2  |  2  |  2  |\n" +
                " 20  |  2  |  2  |  2  |\n" +
                " 20  |  2  |  2  |  2  |\n" +
                " 30  |  3  |  3  |  3  |\n" +
                " 40  |NULL |  4  |NULL |\n" +
                "NULL |NULL |NULL |  4  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testFullJoinWithDerivedTableAndEqualityCondition() throws Exception {
        String sqlText = format("select * from t1 full join (select a2, count(*) as CC from t2 group by a2) dt--splice-properties useSpark=%s, joinStrategy=%s\n on c1=CC", useSpark, joinStrategy);
        String expected = "A1 |B1 | C1  | A2  | CC  |\n" +
                "--------------------------\n" +
                " 1 |10 |  1  |  3  |  1  |\n" +
                " 1 |10 |  1  |  4  |  1  |\n" +
                " 1 |10 |  1  |  5  |  1  |\n" +
                " 1 |10 |  1  |  6  |  1  |\n" +
                " 2 |20 |  2  |  2  |  2  |\n" +
                " 2 |20 |  2  |  2  |  2  |\n" +
                " 3 |30 |  3  |NULL |NULL |\n" +
                " 4 |40 |NULL |NULL |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testFullJoinWithDerivedTableAndInEqualityCondition() throws Exception {
        String sqlText = format("select * from t1 full join (select a2, count(*) as CC from t2 group by a2) dt--splice-properties useSpark=%s, joinStrategy=%s\n on c1<CC", useSpark, joinStrategy);

        String expected = "A1  | B1  | C1  | A2  | CC  |\n" +
                "------------------------------\n" +
                "  1  | 10  |  1  |  2  |  2  |\n" +
                "  2  | 20  |  2  |NULL |NULL |\n" +
                "  2  | 20  |  2  |NULL |NULL |\n" +
                "  3  | 30  |  3  |NULL |NULL |\n" +
                "  4  | 40  |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |  3  |  1  |\n" +
                "NULL |NULL |NULL |  4  |  1  |\n" +
                "NULL |NULL |NULL |  5  |  1  |\n" +
                "NULL |NULL |NULL |  6  |  1  |";

        try {
            ResultSet rs = methodWatcher.executeQuery(sqlText);
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
        }catch (SQLException e) {
            if (joinStrategy.equals("sortmerge"))
                Assert.assertTrue("Invalid exception thrown: " + e, e.getMessage().startsWith("No valid execution plan"));
            else
                Assert.fail("Unexpected exception: " + e.getMessage() + "... JoinStrategy=" + joinStrategy + ", useSpark=" + useSpark);
        }
    }

    @Test
    public void testFullJoinWithOrderBy() throws Exception {
        // plan should not skip the OrderBy operation
        String sqlText = format("select * from t4 full join t5 --splice-properties useSpark=%s, joinStrategy=%s\n on a4=a5 order by a4", useSpark, joinStrategy);
        thirdRowContainsQuery("explain " + sqlText, "OrderBy", methodWatcher);

        String expected = "A4  | B4  | C4  | A5  | B5  | C5  |\n" +
                "------------------------------------\n" +
                "  1  | 10  |  1  |NULL |NULL |NULL |\n" +
                "  2  | 20  |  2  |  2  | 20  |  2  |\n" +
                " 20  | 20  |  2  | 20  | 20  |  2  |\n" +
                "  3  | 30  |  3  |  3  | 30  |  3  |\n" +
                "  4  | 40  |NULL |  4  | 40  |NULL |\n" +
                "NULL |NULL |NULL |  5  | 50  |NULL |\n" +
                "NULL |NULL |NULL |  6  | 60  |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testFullJoinWithIndexAndOrderBy() throws Exception {
        // plan should not skip the OrderBy operation
        String sqlText = format("select b4,c4, b5, c5 from t4 --splice-properties index=idx_t4\n " +
                "full join t5 --splice-properties index=idx_t5, useSpark=%s, joinStrategy=%s\n on b4=b5 order by b4", useSpark, joinStrategy);
        thirdRowContainsQuery("explain " + sqlText, "OrderBy", methodWatcher);

        String expected = "B4  | C4  | B5  | C5  |\n" +
                "------------------------\n" +
                " 10  |  1  |NULL |NULL |\n" +
                " 20  |  2  | 20  |  2  |\n" +
                " 20  |  2  | 20  |  2  |\n" +
                " 20  |  2  | 20  |  2  |\n" +
                " 20  |  2  | 20  |  2  |\n" +
                " 30  |  3  | 30  |  3  |\n" +
                " 40  |NULL | 40  |NULL |\n" +
                "NULL |NULL | 50  |NULL |\n" +
                "NULL |NULL | 60  |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testLeftJoinWithFullOuterJoinAsInnerAndOrderBy() throws Exception {
        // plan should be able to skip the OrderBy operation
        String sqlText = format("select a4, b4 from t4 left join (select X.a4, Y.a5 from t4 as X full join t5 as Y --splice-properties useSpark=%s, joinStrategy=%s\n" +
                "on a4=a5) dt(a,b) --splice-properties joinStrategy=broadcast\n " +
                "on a4=dt.a order by a4", useSpark, joinStrategy);
        queryDoesNotContainString("explain " + sqlText, "OrderBy", methodWatcher);

        String expected = "A4 |B4 |\n" +
                "--------\n" +
                " 1 |10 |\n" +
                " 2 |20 |\n" +
                "20 |20 |\n" +
                " 3 |30 |\n" +
                " 4 |40 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testLeftIsNonCoveringIndex() throws Exception {
        String sqlText = format("select * from t11 --splice-properties index=idx_t11\n " +
                "full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on b1=b2", useSpark, joinStrategy);

        String expected = "A1  | B1  | C1  | A2  | B2  | C2  |\n" +
                "------------------------------------\n" +
                "  1  | 10  |  1  |NULL |NULL |NULL |\n" +
                "  2  | 20  |  2  |  2  | 20  |  2  |\n" +
                "  2  | 20  |  2  |  2  | 20  |  2  |\n" +
                "  2  | 20  |  2  |  2  | 20  |  2  |\n" +
                "  2  | 20  |  2  |  2  | 20  |  2  |\n" +
                "  3  | 30  |  3  |  3  | 30  |  3  |\n" +
                "  4  | 40  |NULL |  4  | 40  |NULL |\n" +
                "NULL |NULL |NULL |  5  | 50  |NULL |\n" +
                "NULL |NULL |NULL |  6  | 60  |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testLeftIsNonCoveringIndex2() throws Exception {
        /* join condition is not covered by the index */
        String sqlText = format("select * from t11 --splice-properties index=idx_t11\n " +
                "full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on c1=c2", useSpark, joinStrategy);

        String expected = "A1  | B1  | C1  | A2  | B2  | C2  |\n" +
                "------------------------------------\n" +
                "  1  | 10  |  1  |NULL |NULL |NULL |\n" +
                "  2  | 20  |  2  |  2  | 20  |  2  |\n" +
                "  2  | 20  |  2  |  2  | 20  |  2  |\n" +
                "  2  | 20  |  2  |  2  | 20  |  2  |\n" +
                "  2  | 20  |  2  |  2  | 20  |  2  |\n" +
                "  3  | 30  |  3  |  3  | 30  |  3  |\n" +
                "  4  | 40  |NULL |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |  4  | 40  |NULL |\n" +
                "NULL |NULL |NULL |  5  | 50  |NULL |\n" +
                "NULL |NULL |NULL |  6  | 60  |  6  |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testLeftIsNonCoveringIndex3() throws Exception {
        /* join condition is not covered by the index and is inequality join condition */
        String sqlText = format("select * from t11 --splice-properties index=idx_t11\n " +
                "full join t2 --splice-properties useSpark=%s, joinStrategy=%s\n on c1>c2", useSpark, joinStrategy);

        String expected = "A1  | B1  | C1  | A2  | B2  | C2  |\n" +
                "------------------------------------\n" +
                "  1  | 10  |  1  |NULL |NULL |NULL |\n" +
                "  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                "  2  | 20  |  2  |NULL |NULL |NULL |\n" +
                "  3  | 30  |  3  |  2  | 20  |  2  |\n" +
                "  3  | 30  |  3  |  2  | 20  |  2  |\n" +
                "  4  | 40  |NULL |NULL |NULL |NULL |\n" +
                "NULL |NULL |NULL |  3  | 30  |  3  |\n" +
                "NULL |NULL |NULL |  4  | 40  |NULL |\n" +
                "NULL |NULL |NULL |  5  | 50  |NULL |\n" +
                "NULL |NULL |NULL |  6  | 60  |  6  |";

        try {
            ResultSet rs = methodWatcher.executeQuery(sqlText);
            Assert.assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
        }catch (SQLException e) {
            if (joinStrategy.equals("sortmerge"))
                Assert.assertTrue("Invalid exception thrown: " + e, e.getMessage().startsWith("No valid execution plan"));
            else
                Assert.fail("Unexpected exception: " + e.getMessage() + "... JoinStrategy=" + joinStrategy + ", useSpark=" + useSpark);
        }
    }
}
