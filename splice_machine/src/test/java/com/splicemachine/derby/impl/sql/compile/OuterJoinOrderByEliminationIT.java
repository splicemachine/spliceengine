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
package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
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

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Created by yxia on 12/15/17.
 */
@RunWith(Parameterized.class)
public class OuterJoinOrderByEliminationIT extends SpliceUnitTest {
    public static final String CLASS_NAME = OuterJoinOrderByEliminationIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(8);
        params.add(new Object[]{"NESTEDLOOP","true"});
        params.add(new Object[]{"SORTMERGE","true"});
        params.add(new Object[]{"BROADCAST","true"});
        params.add(new Object[]{"MERGE","true"});
        params.add(new Object[]{"NESTEDLOOP","false"});
        params.add(new Object[]{"SORTMERGE","false"});
        params.add(new Object[]{"BROADCAST","false"});
        params.add(new Object[]{"MERGE","false"});
        return params;
    }

    private String joinStrategy;
    private String useSparkString;

    public OuterJoinOrderByEliminationIT(String joinStrategy, String useSparkString) {
        this.joinStrategy = joinStrategy;
        this.useSparkString = useSparkString;
    }

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
                .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1 (a1 int, b1 int, c1 int, d1 int, e1 int, primary key (a1,b1))")
                .withIndex("create index idx_t1 on t1(b1,c1,d1)")
                .withIndex("create index idx_t1_2 on t1 (d1 desc, c1 desc, b1 desc)")
                .withInsert("insert into t1 values(?,?,?,?,?)")
                .withRows(rows(
                        row(1, 1, 1, 1, 1),
                        row(2, 2, 3, 30, 300),
                        row(3, 2, 3, 40, 400),
                        row(1, 2, 3, 40, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t2 (a2 int, b2 int, c2 int, d2 int, e2 int, primary key (a2,b2))")
                .withIndex("create index idx_t2 on t2(b2,c2,d2)")
                .withIndex("create index idx_t2_2 on t2 (d2 desc, c2 desc, b2 desc)")
                .withInsert("insert into t2 values(?,?,?,?,?)")
                .withRows(rows(
                        row(1, 1, 1, 1, 1),
                        row(2, 2, 3, 30, 300),
                        row(1, 2, 3, 40, null)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t3 (a3 int, b3 int, c3 int, d3 int, e3 int, primary key (a3,b3))")
                .withIndex("create index idx_t3 on t3(b3,c3,d3)")
                .withIndex("create index idx_t3_2 on t3 (d3 desc, c3 desc, b3 desc)")
                .withInsert("insert into t3 values(?,?,?,?,?)")
                .withRows(rows(
                        row(1, 1, 1, 1, 1),
                        row(2, 2, 3, 30, 300),
                        row(1, 2, 3, 40, null)))
                .create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testStandAloneOuterJoinWithPrimaryKey() throws Exception {
        /* Q1 single outer join with join colunmn same as the column table is ordered on */
        String sqlText = format("select a1,b1 from t1 --splice-properties index=null \n" +
                        "left join t2 --splice-properties index=null, joinStrategy=%s, useSpark=%s\n " +
                        "on a1=a2 order by a1,b1 {limit 20}",
                this.joinStrategy, this.useSparkString);
        String expected = "A1 |B1 |\n" +
                "--------\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 2 |\n" +
                " 1 | 2 |\n" +
                " 2 | 2 |\n" +
                " 3 | 2 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        if (!joinStrategy.equals("SORTMERGE"))
            queryDoesNotContainString("explain " + sqlText, "OrderBy", methodWatcher);
        else
            rowContainsQuery(4, "explain "+sqlText, "OrderBy", methodWatcher);

        /* Q1-1 extend the select list to just make sure the content is computed correctly regardless of the order*/
        sqlText = format("select a1,b1,a2,b2 from t1 --splice-properties index=null \n" +
                        "left join t2 --splice-properties index=null, joinStrategy=%s, useSpark=%s\n " +
                        "on a1=a2 order by a1,b1 {limit 20}",
                this.joinStrategy, this.useSparkString);
        expected = "A1 |B1 | A2  | B2  |\n" +
                "--------------------\n" +
                " 1 | 1 |  1  |  1  |\n" +
                " 1 | 1 |  1  |  2  |\n" +
                " 1 | 2 |  1  |  1  |\n" +
                " 1 | 2 |  1  |  2  |\n" +
                " 2 | 2 |  2  |  2  |\n" +
                " 3 | 2 |NULL |NULL |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));

        /* Q2 join on a different column */
        sqlText = format("select a1,b1 from t1 --splice-properties index=null \n" +
                        "left join t2 --splice-properties index=null, joinStrategy=%s, useSpark=%s\n " +
                        "on b1=a2 order by a1 {limit 20}",
                this.joinStrategy, this.useSparkString);
        expected = "A1 |B1 |\n" +
                "--------\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 2 |\n" +
                " 2 | 2 |\n" +
                " 3 | 2 |";

        try {
            rs = methodWatcher.executeQuery(sqlText);
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();
            if (joinStrategy.equals("MERGE"))
                Assert.fail("Invalid join plan, we shouldn't get here");
            else if (joinStrategy.equals("NESTEDLOOP") || joinStrategy.equals("BROADCAST"))
                queryDoesNotContainString("explain " + sqlText, "OrderBy", methodWatcher);
            else
                rowContainsQuery(4, "explain " + sqlText, "OrderBy", methodWatcher);
        } catch (SQLException e) {
            if (joinStrategy.equals("MREGE"))
                assertEquals("42Y69", e.getSQLState());
        }

        /* Q2-1 join on a different column, just check the content of the result with an extended select list */
        sqlText = format("select a1,b1,a2,b2 from t1 --splice-properties index=null \n" +
                        "left join t2 --splice-properties index=null, joinStrategy=%s, useSpark=%s\n " +
                        "on b1=a2 order by a1 {limit 20}",
                this.joinStrategy, this.useSparkString);
        expected = "A1 |B1 |A2 |B2 |\n" +
                "----------------\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 | 2 |\n" +
                " 1 | 2 | 2 | 2 |\n" +
                " 2 | 2 | 2 | 2 |\n" +
                " 3 | 2 | 2 | 2 |";

        try {
            rs = methodWatcher.executeQuery(sqlText);
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
        } catch (SQLException e) {
            if (joinStrategy.equals("MREGE"))
                assertEquals("42Y69", e.getSQLState());
        }

        /* Q3 multiple outer join query */
        sqlText = format("select a1, b1 from t1 --splice-properties index=null\n" +
                        "left join t2 --splice-properties index=null, joinStrategy=%s, useSpark=%s\n " +
                        "on a1=a2 " +
                        "left join t3 --splice-properties index=null, joinStrategy=%s\n " +
                        "on a1=a3 order by a1,b1 {limit 20}",
                this.joinStrategy, this.useSparkString, this.joinStrategy);
        expected = "A1 |B1 |\n" +
                "--------\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 1 |\n" +
                " 1 | 2 |\n" +
                " 1 | 2 |\n" +
                " 1 | 2 |\n" +
                " 1 | 2 |\n" +
                " 2 | 2 |\n" +
                " 3 | 2 |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        if (!joinStrategy.equals("SORTMERGE"))
            queryDoesNotContainString("explain " + sqlText, "OrderBy", methodWatcher);
        else
            rowContainsQuery(4, "explain "+sqlText, "OrderBy", methodWatcher);

        /* Q3-3 just check the content of the result with an extended select list */
        sqlText = format("select a1, b1, a2, b2, a3, b3 from t1 --splice-properties index=null\n" +
                        "left join t2 --splice-properties index=null, joinStrategy=%s, useSpark=%s\n " +
                        "on a1=a2 " +
                        "left join t3 --splice-properties index=null, joinStrategy=%s\n " +
                        "on a1=a3 order by a1,b1 {limit 20}",
                this.joinStrategy, this.useSparkString, this.joinStrategy);
        expected = "A1 |B1 | A2  | B2  | A3  | B3  |\n" +
                "--------------------------------\n" +
                " 1 | 1 |  1  |  1  |  1  |  1  |\n" +
                " 1 | 1 |  1  |  1  |  1  |  2  |\n" +
                " 1 | 1 |  1  |  2  |  1  |  1  |\n" +
                " 1 | 1 |  1  |  2  |  1  |  2  |\n" +
                " 1 | 2 |  1  |  1  |  1  |  1  |\n" +
                " 1 | 2 |  1  |  1  |  1  |  2  |\n" +
                " 1 | 2 |  1  |  2  |  1  |  1  |\n" +
                " 1 | 2 |  1  |  2  |  1  |  2  |\n" +
                " 2 | 2 |  2  |  2  |  2  |  2  |\n" +
                " 3 | 2 |NULL |NULL |NULL |NULL |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        /* Q4 inner join inside the outer table of the left join */
        sqlText = format("select a1,b1,a2,a3 from t1 --splice-properties index=null \n" +
                         "inner join t3 --splice-properties index=null, joinStrategy=%s\n on a1=a3 " +
                        "left join t2 --splice-properties index=null, joinStrategy=%s, useSpark=%s\n " +
                        "on a1=a2 order by a1 {limit 20}",
                this.joinStrategy, this.joinStrategy, this.useSparkString);
        expected = "A1 |B1 |A2 |A3 |\n" +
                "----------------\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        if (!joinStrategy.equals("SORTMERGE"))
            queryDoesNotContainString("explain " + sqlText, "OrderBy", methodWatcher);
        else
            rowContainsQuery(4, "explain " + sqlText, "OrderBy", methodWatcher);

        /* Q4-1 inner join inside the inner table of the left join */
        sqlText = format("select a1,b1,a2,a3 from t1 --splice-properties index=null \n" +
                        "left join t2 --splice-properties index=null\n" +
                        "inner join t3 --splice-properties index=null, joinStrategy=%s, useSpark=%s\n" +
                        "on a2=a3 on a1=a2 order by a1 {limit 20}",
                this.joinStrategy, this.useSparkString);
        expected = "A1 |B1 | A2  | A3  |\n" +
                "--------------------\n" +
                " 1 | 1 |  1  |  1  |\n" +
                " 1 | 1 |  1  |  1  |\n" +
                " 1 | 1 |  1  |  1  |\n" +
                " 1 | 1 |  1  |  1  |\n" +
                " 1 | 2 |  1  |  1  |\n" +
                " 1 | 2 |  1  |  1  |\n" +
                " 1 | 2 |  1  |  1  |\n" +
                " 1 | 2 |  1  |  1  |\n" +
                " 2 | 2 |  2  |  2  |\n" +
                " 3 | 2 |NULL |NULL |";

        try {
            rs = methodWatcher.executeQuery(sqlText);
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();
            queryDoesNotContainString("explain " + sqlText, "OrderBy", methodWatcher);
        } catch (SQLException e) {
            if (joinStrategy.equals("MREGE"))
                assertEquals("42Y69", e.getSQLState());
        }

        /* Q5 left join is the first in the list */ /* test merge join */
        sqlText = format("select a1,b1,a2,a3 from --splice-properties joinOrder=fixed\n" +
                        "t1 --splice-properties index=null \n" +
                        "left join t2 --splice-properties index=null, joinStrategy=%s, useSpark=%s\n " +
                        "on a1=a2, t3 --splice-properties index=null, joinStrategy=%s\n where a1=a3 order by a1 {limit 20}",
                this.joinStrategy, this.useSparkString, this.joinStrategy);
        expected = "A1 |B1 |A2 |A3 |\n" +
                "----------------\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        if (!joinStrategy.equals("SORTMERGE"))
            queryDoesNotContainString("explain " + sqlText, "OrderBy", methodWatcher);
        else
            rowContainsQuery(4, "explain " + sqlText, "OrderBy", methodWatcher);

        /* Q6: whole thing in a derived table */
        sqlText = format("select * from (select a1,b1,a2,a3 from --splice-properties joinOrder=fixed\n" +
                        "t1 --splice-properties index=null \n" +
                        "left join t2 --splice-properties index=null, joinStrategy=%s, useSpark=%s\n " +
                        "on a1=a2, t3 --splice-properties index=null, joinStrategy=%s\n where a1=a3 order by a1 {limit 20}) dt",
                this.joinStrategy, this.useSparkString, this.joinStrategy);
        expected = "A1 |B1 |A2 |A3 |\n" +
                "----------------\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        if (!joinStrategy.equals("SORTMERGE"))
            queryDoesNotContainString("explain " + sqlText, "OrderBy", methodWatcher);
        else
            rowContainsQuery(4, "explain " + sqlText, "OrderBy", methodWatcher);

        /* Q7: whole thing in a union-all */
        sqlText = format("select * from (select a1,b1,a2,a3 from --splice-properties joinOrder=fixed\n" +
                        "t1 --splice-properties index=null \n" +
                        "left join t2 --splice-properties index=null, joinStrategy=%s, useSpark=%s\n " +
                        "on a1=a2, t3 --splice-properties index=null, joinStrategy=%s\n where a1=a3 order by a1 {limit 20}) dt union all " +
                        "values (9,9,9,9)",
                this.joinStrategy, this.useSparkString, this.joinStrategy);
        expected = "1 | 2 | 3 | 4 |\n" +
                "----------------\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 |\n" +
                " 9 | 9 | 9 | 9 |";
        // With DB-8132, the order between the union-all branch is not preserved, so we have an alternative
        // exepcted result
        String expected1 = "1 | 2 | 3 | 4 |\n" +
                "----------------\n" +
                " 9 | 9 | 9 | 9 |\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 |";


        rs = methodWatcher.executeQuery(sqlText);
        String actual = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        Assert.assertTrue("result mismatch, actual is: "+actual,
                actual != null && (expected.equals(actual) || expected1.equals(actual)));

        rs.close();
        if (!joinStrategy.equals("SORTMERGE"))
            queryDoesNotContainString("explain " + sqlText, "OrderBy", methodWatcher);
        else
            rowContainsQuery(6, "explain " + sqlText, "OrderBy", methodWatcher);

        /* NG-1: left table is a derived table */
        sqlText = format("select a1,b1,a2,a3 from (select * from t1 --splice-properties index=null \n" +
                        "inner join t3 --splice-properties index=null, joinStrategy=%s\n on a1=a3) dt " +
                        "left join t2 --splice-properties index=null, joinStrategy=%s, useSpark=%s\n " +
                        "on a1=a2 order by a1 {limit 20}",
                this.joinStrategy, this.joinStrategy, this.useSparkString);

        try {
            rowContainsQuery(4, "explain " + sqlText, "OrderBy", methodWatcher);
            expected = "A1 |B1 |A2 |A3 |\n" +
                    "----------------\n" +
                    " 1 | 1 | 1 | 1 |\n" +
                    " 1 | 1 | 1 | 1 |\n" +
                    " 1 | 1 | 1 | 1 |\n" +
                    " 1 | 1 | 1 | 1 |\n" +
                    " 1 | 2 | 1 | 1 |\n" +
                    " 1 | 2 | 1 | 1 |\n" +
                    " 1 | 2 | 1 | 1 |\n" +
                    " 1 | 2 | 1 | 1 |\n" +
                    " 2 | 2 | 2 | 2 |";

            rs = methodWatcher.executeQuery(sqlText);
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
        } catch (SQLException e) {
            if (joinStrategy.equals("MREGE"))
                assertEquals("42Y69", e.getSQLState());
        }

        /* NG-2: order by skip the inner table of the outer join */
        /* t1 left join t2, t3 order by t1,t3 */
        sqlText = format("select a1,b1,a2,a3 from --splice-properties joinOrder=fixed\n" +
                        "t1 --splice-properties index=null \n" +
                        "left join t2 --splice-properties index=null, joinStrategy=%s, useSpark=%s\n " +
                        "on a1=a2, t3 --splice-properties index=null, joinStrategy=%s\n where a1=a3 order by a1,b1, a3, b3 {limit 20}",
                this.joinStrategy, this.useSparkString, this.joinStrategy);

        rowContainsQuery(5, "explain " + sqlText, "OrderBy", methodWatcher);
        expected = "A1 |B1 |A2 |A3 |\n" +
                "----------------\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 1 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 1 | 2 | 1 | 1 |\n" +
                " 2 | 2 | 2 | 2 |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }


    @Test
    public void testStandAloneOuterJoinWithIndex() throws Exception {
        /* Q1: index column with desc order */
        String sqlText = format("select d1,c1, b1 from t1 --splice-properties index=idx_t1_2 \n" +
                        "left join t2 --splice-properties index=idx_t2_2, joinStrategy=%s, useSpark=%s\n " +
                        "on d1=d2 where c1 in (1,3) order by d1 desc, c1 desc, b1 desc{limit 20}",
                this.joinStrategy, this.useSparkString);
        String expected = "D1 |C1 |B1 |\n" +
                "------------\n" +
                "40 | 3 | 2 |\n" +
                "40 | 3 | 2 |\n" +
                "30 | 3 | 2 |\n" +
                " 1 | 1 | 1 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        if (!joinStrategy.equals("SORTMERGE"))
            queryDoesNotContainString("explain " + sqlText, "OrderBy", methodWatcher);
        else
            rowContainsQuery(4, "explain "+sqlText, "OrderBy", methodWatcher);



        /* Q2 : join on a column different than the index column */
        sqlText = format("select d1,c1, b1 from t1 --splice-properties index=idx_t1_2 \n" +
                        "left join t2 --splice-properties index=idx_t2_2, joinStrategy=%s, useSpark=%s\n " +
                        "on a1=a2 where c1=3 order by d1 desc, b1 desc{limit 20}",
                this.joinStrategy, this.useSparkString);
        expected = "D1 |C1 |B1 |\n" +
                "------------\n" +
                "40 | 3 | 2 |\n" +
                "40 | 3 | 2 |\n" +
                "40 | 3 | 2 |\n" +
                "30 | 3 | 2 |";

        try {
            rs = methodWatcher.executeQuery(sqlText);
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
            rs.close();
            if (joinStrategy.equals("MERGE"))
                Assert.fail("Invalid join plan, we shouldn't get here");
            else if (joinStrategy.equals("NESTEDLOOP") || joinStrategy.equals("BROADCAST"))
                queryDoesNotContainString("explain " + sqlText, "OrderBy", methodWatcher);
            else
                rowContainsQuery(4, "explain " + sqlText, "OrderBy", methodWatcher);
        } catch (SQLException e) {
            if (joinStrategy.equals("MREGE"))
                assertEquals("42Y69", e.getSQLState());
        }


        /* Q2-1 extend the select list to just make sure the content is computed correctly regardless of the order*/
        sqlText = format("select * from t1 --splice-properties index=idx_t1_2 \n" +
                        "left join t2 --splice-properties index=idx_t2_2, joinStrategy=%s, useSpark=%s\n " +
                        "on a1=a2 where c1 =3 order by d1 desc, b1 desc{limit 20}",
                this.joinStrategy, this.useSparkString);
        expected = "A1 |B1 |C1 |D1 | E1  | A2  | B2  | C2  | D2  | E2  |\n" +
                "----------------------------------------------------\n" +
                " 1 | 2 | 3 |40 |NULL |  1  |  1  |  1  |  1  |  1  |\n" +
                " 1 | 2 | 3 |40 |NULL |  1  |  2  |  3  | 40  |NULL |\n" +
                " 2 | 2 | 3 |30 | 300 |  2  |  2  |  3  | 30  | 300 |\n" +
                " 3 | 2 | 3 |40 | 400 |NULL |NULL |NULL |NULL |NULL |";

        try {
            rs = methodWatcher.executeQuery(sqlText);
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        } catch (SQLException e) {
            if (joinStrategy.equals("MREGE"))
                assertEquals("42Y69", e.getSQLState());
        }

        /* Q4: index lookup + multiple outer join + inlist + order by + skip ordered column with constant value + limit */
        sqlText = format("select a1, b1, c1, d1 from t1 --splice-properties index=idx_t1\n" +
                        "left join t2 --splice-properties index=idx_t2, joinStrategy=%s, useSpark=%s\n " +
                        "on b1=b2 " +
                        "left join t3 --splice-properties index=idx_t3, joinStrategy=%s\n " +
                        "on b1=b3 " +
                        "where b1 in (1,2) and c1=3 and d1 > 0 order by b1, d1 {limit 20}",
                this.joinStrategy, this.useSparkString, this.joinStrategy);
        expected = "A1 |B1 |C1 |D1 |\n" +
                "----------------\n" +
                " 2 | 2 | 3 |30 |\n" +
                " 2 | 2 | 3 |30 |\n" +
                " 2 | 2 | 3 |30 |\n" +
                " 2 | 2 | 3 |30 |\n" +
                " 1 | 2 | 3 |40 |\n" +
                " 1 | 2 | 3 |40 |\n" +
                " 1 | 2 | 3 |40 |\n" +
                " 1 | 2 | 3 |40 |\n" +
                " 3 | 2 | 3 |40 |\n" +
                " 3 | 2 | 3 |40 |\n" +
                " 3 | 2 | 3 |40 |\n" +
                " 3 | 2 | 3 |40 |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        if (!joinStrategy.equals("SORTMERGE"))
            queryDoesNotContainString("explain " + sqlText, "OrderBy", methodWatcher);
        else
            rowContainsQuery(4, "explain "+sqlText, "OrderBy", methodWatcher);

        /* Q4-1 extend the select list to verify the join result's content */
        sqlText = format("select a1, b1, c1, d1, a2, b2, c2, d2, a3, b3, c3, d3 from t1 --splice-properties index=idx_t1\n" +
                        "left join t2 --splice-properties index=idx_t2, joinStrategy=%s, useSpark=%s\n " +
                        "on b1=b2 " +
                        "left join t3 --splice-properties index=idx_t3, joinStrategy=%s\n " +
                        "on b1=b3 " +
                        "where b1 in (1,2) and c1=3 and d1 > 0 order by b1, d1 {limit 20}",
                this.joinStrategy, this.useSparkString, this.joinStrategy);
        expected = "A1 |B1 |C1 |D1 |A2 |B2 |C2 |D2 |A3 |B3 |C3 |D3 |\n" +
                "------------------------------------------------\n" +
                " 1 | 2 | 3 |40 | 1 | 2 | 3 |40 | 1 | 2 | 3 |40 |\n" +
                " 1 | 2 | 3 |40 | 1 | 2 | 3 |40 | 2 | 2 | 3 |30 |\n" +
                " 1 | 2 | 3 |40 | 2 | 2 | 3 |30 | 1 | 2 | 3 |40 |\n" +
                " 1 | 2 | 3 |40 | 2 | 2 | 3 |30 | 2 | 2 | 3 |30 |\n" +
                " 2 | 2 | 3 |30 | 1 | 2 | 3 |40 | 1 | 2 | 3 |40 |\n" +
                " 2 | 2 | 3 |30 | 1 | 2 | 3 |40 | 2 | 2 | 3 |30 |\n" +
                " 2 | 2 | 3 |30 | 2 | 2 | 3 |30 | 1 | 2 | 3 |40 |\n" +
                " 2 | 2 | 3 |30 | 2 | 2 | 3 |30 | 2 | 2 | 3 |30 |\n" +
                " 3 | 2 | 3 |40 | 1 | 2 | 3 |40 | 1 | 2 | 3 |40 |\n" +
                " 3 | 2 | 3 |40 | 1 | 2 | 3 |40 | 2 | 2 | 3 |30 |\n" +
                " 3 | 2 | 3 |40 | 2 | 2 | 3 |30 | 1 | 2 | 3 |40 |\n" +
                " 3 | 2 | 3 |40 | 2 | 2 | 3 |30 | 2 | 2 | 3 |30 |";

        try {
            rs = methodWatcher.executeQuery(sqlText);
            Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        } catch (SQLException e) {
            if (joinStrategy.equals("MREGE"))
                assertEquals("42Y69", e.getSQLState());
        }
    }

    /* This test case is pulled out from testStandAloneOuterJoinWithIndex() as they are affected by
       wrong result tracked in SPLICE-1982.
     */
    @Ignore("SPLICE-1982")
    @Test
    public void testResultOnIndexWithDescendingOrder() throws Exception{
        /* Q1-1 extend the select list to just make sure the content is computed correctly regardless of the order*/
        String sqlText = format("select d1,c1, b1, d2, c2, b2 from t1 --splice-properties index=idx_t1_2 \n" +
                        "left join t2 --splice-properties index=idx_t2_2, joinStrategy=%s, useSpark=%s\n " +
                        "on d1=d2 where c1 in (1,3) order by d1 desc, c1 desc, b1 desc {limit 20}",
                this.joinStrategy, this.useSparkString);
        String expected = "D1 |C1 |B1 |D2 |C2 |B2 |\n" +
                "------------------------\n" +
                " 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                "30 | 3 | 2 |30 | 3 | 2 |\n" +
                "40 | 3 | 2 |40 | 3 | 2 |\n" +
                "40 | 3 | 2 |40 | 3 | 2 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        // note toString() will sort result set in ascending order
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));

         /* Q3: negative test case, one order by column is in ascending order, sort cannot be avoided */
        sqlText = format("select d1,c1, b1, d2,c2, b2 from t1 --splice-properties index=idx_t1_2 \n" +
                        "left join t2 --splice-properties index=idx_t2_2, joinStrategy=%s, useSpark=%s\n " +
                        "on d1=d2 where c1 in (1,3) order by d1 desc, c1 desc, b1 asc{limit 20}",
                this.joinStrategy, this.useSparkString);
        expected = "D1 |C1 |B1 |D2 |C2 |B2 |\n" +
                "------------------------\n" +
                "40 | 3 | 2 |40 | 3 | 2 |\n" +
                "40 | 3 | 2 |40 | 3 | 2 |\n" +
                "30 | 3 | 2 |30 | 3 | 2 |\n" +
                " 1 | 1 | 1 | 1 | 1 | 1 |";

        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
        rowContainsQuery(4, "explain "+sqlText, "OrderBy", methodWatcher);
    }
}
