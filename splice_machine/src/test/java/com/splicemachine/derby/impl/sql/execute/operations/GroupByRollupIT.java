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

package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by yxia on 1/17/19.
 */
@RunWith(Parameterized.class)
public class GroupByRollupIT extends SpliceUnitTest {
    private static final String SCHEMA = GroupByRollupIT.class.getSimpleName().toUpperCase();
    private static SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"false"});
        params.add(new Object[]{"true"});
        return params;
    }

    private String useSparkString;

    public GroupByRollupIT(String useSparkString) {
        this.useSparkString = useSparkString;
    }

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    @BeforeClass
    public static void createSharedTables() throws Exception {
        TestConnection connection=spliceClassWatcher.getOrCreateConnection();
        new TableCreator(connection)
                .withCreate("create table t1 (a1 int, b1 int, c1 int, d1 int)")
                .withInsert("insert into t1 values(?,?,?,?)")
                .withRows(rows(row(1,1,1,1), row(1,1,1,2), row(1,1,1,3), row(1,1,2,1), row(1,1,2,2), row(1,1,2,3),
                               row(1,2,1,1), row(1,2,1,2), row(1,2,1,3), row(1,2,2,1), row(1,2,2,2), row(1,2,2,3),
                               row(1,null,1,1), row(1,null,1,2), row(1,null,1,3), row(1,null,2,1), row(1,null,2,2), row(1,null,2,3),
                               row(2,1,1,1), row(2,1,1,2), row(2,1,1,3), row(2,1,2,1), row(2,1,2,2), row(2,1,2,3),
                               row(2,2,1,1), row(2,2,1,2), row(2,2,1,3), row(2,2,2,1), row(2,2,2,2), row(2,2,2,3),
                               row(3,1,1,1), row(3,1,1,2), row(3,1,1,3), row(3,1,2,1), row(3,1,2,2), row(3,1,2,3),
                               row(3,2,1,1), row(3,2,1,2), row(3,2,1,3), row(3,2,2,1), row(3,2,2,2), row(3,2,2,3)))
                .create();

        new TableCreator(connection)
                .withCreate("create table t2 (a2 int, b2 int, c2 int, d2 int)")
                .withInsert("insert into t2 values(?,?,?,?)")
                .withRows(rows(row(1,1,1,1), row(1,1,1,2), row(1,2,1,1), row(1,2,1,2)))
                .create();
    }

    @Test
    public void testRollupWithSingleGroup() throws Exception {
        String sqlText = format("select a1, count(*), grouping(a1) from t1 --splice-properties useSpark=%s\n" +
                "group by rollup(a1) order by 1, 3", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A1  | 2 | 3 |\n" +
                "--------------\n" +
                "  1  |18 | 0 |\n" +
                "  2  |12 | 0 |\n" +
                "  3  |12 | 0 |\n" +
                "NULL |42 | 1 |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRollupWithMultipleGroupsAndNulValue() throws Exception {
        String sqlText = format("select a1, b1, count(*), grouping(a1), grouping(b1) from t1 --splice-properties useSpark=%s\n" +
                "group by rollup(a1,b1) order by 1,2,4,5", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A1  | B1  | 3 | 4 | 5 |\n" +
                "------------------------\n" +
                "  1  |  1  | 6 | 0 | 0 |\n" +
                "  1  |  2  | 6 | 0 | 0 |\n" +
                "  1  |NULL | 6 | 0 | 0 |\n" +
                "  1  |NULL |18 | 0 | 1 |\n" +
                "  2  |  1  | 6 | 0 | 0 |\n" +
                "  2  |  2  | 6 | 0 | 0 |\n" +
                "  2  |NULL |12 | 0 | 1 |\n" +
                "  3  |  1  | 6 | 0 | 0 |\n" +
                "  3  |  2  | 6 | 0 | 0 |\n" +
                "  3  |NULL |12 | 0 | 1 |\n" +
                "NULL |NULL |42 | 1 | 1 |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testGroupingFunctionInExpression() throws Exception {
        String sqlText = format("select a1, b1, count(*), grouping(a1), grouping(b1), grouping(a1) + grouping(b1) from t1 --splice-properties useSpark=%s\n" +
                "group by rollup(a1,b1) order by 1,2,4,5", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A1  | B1  | 3 | 4 | 5 | 6 |\n" +
                "----------------------------\n" +
                "  1  |  1  | 6 | 0 | 0 | 0 |\n" +
                "  1  |  2  | 6 | 0 | 0 | 0 |\n" +
                "  1  |NULL | 6 | 0 | 0 | 0 |\n" +
                "  1  |NULL |18 | 0 | 1 | 1 |\n" +
                "  2  |  1  | 6 | 0 | 0 | 0 |\n" +
                "  2  |  2  | 6 | 0 | 0 | 0 |\n" +
                "  2  |NULL |12 | 0 | 1 | 1 |\n" +
                "  3  |  1  | 6 | 0 | 0 | 0 |\n" +
                "  3  |  2  | 6 | 0 | 0 | 0 |\n" +
                "  3  |NULL |12 | 0 | 1 | 1 |\n" +
                "NULL |NULL |42 | 1 | 1 | 2 |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testGroupingFunctionInHavingClause() throws Exception {
        String sqlText = format("select a1, b1, count(*), grouping(a1), grouping(b1) from t1 --splice-properties useSpark=%s\n" +
                "group by rollup(b1,a1) order by 2,1,5,4", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A1  | B1  | 3 | 4 | 5 |\n" +
                "------------------------\n" +
                "  1  |  1  | 6 | 0 | 0 |\n" +
                "  2  |  1  | 6 | 0 | 0 |\n" +
                "  3  |  1  | 6 | 0 | 0 |\n" +
                "NULL |  1  |18 | 1 | 0 |\n" +
                "  1  |  2  | 6 | 0 | 0 |\n" +
                "  2  |  2  | 6 | 0 | 0 |\n" +
                "  3  |  2  | 6 | 0 | 0 |\n" +
                "NULL |  2  |18 | 1 | 0 |\n" +
                "  1  |NULL | 6 | 0 | 0 |\n" +
                "NULL |NULL | 6 | 1 | 0 |\n" +
                "NULL |NULL |42 | 1 | 1 |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = format("select a1, b1, count(*), grouping(a1), grouping(b1) from t1 --splice-properties useSpark=%s\n" +
                "group by rollup(b1,a1) having grouping(a1)=1 order by 2,1,5,4", this.useSparkString);

        rs = methodWatcher.executeQuery(sqlText);

        expected = "A1  | B1  | 3 | 4 | 5 |\n" +
                "------------------------\n" +
                "NULL |  1  |18 | 1 | 0 |\n" +
                "NULL |  2  |18 | 1 | 0 |\n" +
                "NULL |NULL | 6 | 1 | 0 |\n" +
                "NULL |NULL |42 | 1 | 1 |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRollupOverJoinResult() throws Exception {
        String sqlText = format("select a1, b2, grouping(a1), grouping(b2), count(*) from t1 left join t2 --splice-properties useSpark=%s\n" +
                "on a1=a2 group by rollup(a1,b2) order by 1,2,4,5", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A1  | B2  | 3 | 4 | 5 |\n" +
                "------------------------\n" +
                "  1  |  1  | 0 | 0 |36 |\n" +
                "  1  |  2  | 0 | 0 |36 |\n" +
                "  1  |NULL | 0 | 1 |72 |\n" +
                "  2  |NULL | 0 | 0 |12 |\n" +
                "  2  |NULL | 0 | 1 |12 |\n" +
                "  3  |NULL | 0 | 0 |12 |\n" +
                "  3  |NULL | 0 | 1 |12 |\n" +
                "NULL |NULL | 1 | 1 |96 |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRollupInDerivedTable() throws Exception {
        String sqlText = format("select a1, b2, count(*)as CountVal, max(d1) as MaxVal, grouping(a1), grouping(b2) from t1 left join t2 --splice-properties useSpark=%s\n" +
                " on a1=a2 group by rollup(a1,b2) order by 1,2,5,6", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A1  | B2  |COUNTVAL |MAXVAL | 5 | 6 |\n" +
                "--------------------------------------\n" +
                "  1  |  1  |   36    |   3   | 0 | 0 |\n" +
                "  1  |  2  |   36    |   3   | 0 | 0 |\n" +
                "  1  |NULL |   72    |   3   | 0 | 1 |\n" +
                "  2  |NULL |   12    |   3   | 0 | 0 |\n" +
                "  2  |NULL |   12    |   3   | 0 | 1 |\n" +
                "  3  |NULL |   12    |   3   | 0 | 0 |\n" +
                "  3  |NULL |   12    |   3   | 0 | 1 |\n" +
                "NULL |NULL |   96    |   3   | 1 | 1 |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        // put aggregate query in a derived table
        sqlText = format("select b2, CountVal, MaxVal, GB from (" +
                "select a1, b2, count(*)as CountVal, max(d1) as MaxVal, grouping(a1) as GA, grouping(b2) as GB from t1 left join t2 --splice-properties useSpark=%s\n" +
                "on a1=a2 group by rollup(a1,b2)) as v1 " +
                "order by 1,2,4", this.useSparkString);

        rs = methodWatcher.executeQuery(sqlText);

        expected = "B2  |COUNTVAL |MAXVAL |GB |\n" +
                "----------------------------\n" +
                "  1  |   36    |   3   | 0 |\n" +
                "  2  |   36    |   3   | 0 |\n" +
                "NULL |   12    |   3   | 0 |\n" +
                "NULL |   12    |   3   | 0 |\n" +
                "NULL |   12    |   3   | 1 |\n" +
                "NULL |   12    |   3   | 1 |\n" +
                "NULL |   72    |   3   | 1 |\n" +
                "NULL |   96    |   3   | 1 |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        // add where clause to derived table
        sqlText = format("select b2, CountVal, MaxVal, GB from (" +
                "select a1, b2, count(*)as CountVal, max(d1) as MaxVal, grouping(a1) as GA, grouping(b2) as GB from t1 left join t2 --splice-properties useSpark=%s\n" +
                "on a1=a2 group by rollup(a1,b2)) as v1 where GB > 0" +
                "order by 1,2,4", this.useSparkString);

        rs = methodWatcher.executeQuery(sqlText);

        expected = "B2  |COUNTVAL |MAXVAL |GB |\n" +
                "----------------------------\n" +
                "NULL |   12    |   3   | 1 |\n" +
                "NULL |   12    |   3   | 1 |\n" +
                "NULL |   72    |   3   | 1 |\n" +
                "NULL |   96    |   3   | 1 |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRollupWithSingleDistinctAggregate() throws Exception {
        String sqlText = format("select a1, b1, count(distinct c1), grouping(a1), grouping(b1) from t1 --splice-properties useSpark=%s\n" +
                "group by rollup(a1,b1) order by 1,2,4,5", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A1  | B1  | 3 | 4 | 5 |\n" +
                "------------------------\n" +
                "  1  |  1  | 2 | 0 | 0 |\n" +
                "  1  |  2  | 2 | 0 | 0 |\n" +
                "  1  |NULL | 2 | 0 | 0 |\n" +
                "  1  |NULL | 2 | 0 | 1 |\n" +
                "  2  |  1  | 2 | 0 | 0 |\n" +
                "  2  |  2  | 2 | 0 | 0 |\n" +
                "  2  |NULL | 2 | 0 | 1 |\n" +
                "  3  |  1  | 2 | 0 | 0 |\n" +
                "  3  |  2  | 2 | 0 | 0 |\n" +
                "  3  |NULL | 2 | 0 | 1 |\n" +
                "NULL |NULL | 2 | 1 | 1 |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRollupWithMultipleDistinctAggregate() throws Exception {
        String sqlText = format("select a1, b1, count(distinct c1), count(distinct d1), grouping(a1), grouping(b1) from t1 --splice-properties useSpark=%s\n" +
                "group by rollup(a1,b1) order by 1,2,5,6", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A1  | B1  | 3 | 4 | 5 | 6 |\n" +
                "----------------------------\n" +
                "  1  |  1  | 2 | 3 | 0 | 0 |\n" +
                "  1  |  2  | 2 | 3 | 0 | 0 |\n" +
                "  1  |NULL | 2 | 3 | 0 | 0 |\n" +
                "  1  |NULL | 2 | 3 | 0 | 1 |\n" +
                "  2  |  1  | 2 | 3 | 0 | 0 |\n" +
                "  2  |  2  | 2 | 3 | 0 | 0 |\n" +
                "  2  |NULL | 2 | 3 | 0 | 1 |\n" +
                "  3  |  1  | 2 | 3 | 0 | 0 |\n" +
                "  3  |  2  | 2 | 3 | 0 | 0 |\n" +
                "  3  |NULL | 2 | 3 | 0 | 1 |\n" +
                "NULL |NULL | 2 | 3 | 1 | 1 |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testRollupWithoutConversionToDistinct() throws Exception {
        String sqlText = format("select a1 from t1 --splice-properties useSpark=%s\n" +
                "group by rollup(a1) order by 1", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A1  |\n" +
                "------\n" +
                "  1  |\n" +
                "  2  |\n" +
                "  3  |\n" +
                "NULL |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testGroupingFunctionWithWindowFunction() throws Exception {
        String sqlText = format("select a1, b1, grouping(a1), grouping(b1), row_number() over (partition by a1 order by b1 nulls first, grouping(b1)) from t1 --splice-properties useSpark=%s\n" +
                "group by rollup(a1,b1) order by 1,5", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A1  | B1  | 3 | 4 | 5 |\n" +
                "------------------------\n" +
                "  1  |NULL | 0 | 0 | 1 |\n" +
                "  1  |NULL | 0 | 1 | 2 |\n" +
                "  1  |  1  | 0 | 0 | 3 |\n" +
                "  1  |  2  | 0 | 0 | 4 |\n" +
                "  2  |NULL | 0 | 1 | 1 |\n" +
                "  2  |  1  | 0 | 0 | 2 |\n" +
                "  2  |  2  | 0 | 0 | 3 |\n" +
                "  3  |NULL | 0 | 1 | 1 |\n" +
                "  3  |  1  | 0 | 0 | 2 |\n" +
                "  3  |  2  | 0 | 0 | 3 |\n" +
                "NULL |NULL | 1 | 1 | 1 |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = format("select a1, b1, grouping(a1), grouping(b1), count(*), row_number() over (partition by a1 order by count(*),b1) from t1 --splice-properties useSpark=%s\n" +
                "group by rollup(a1,b1) order by 1,6", this.useSparkString);

        rs = methodWatcher.executeQuery(sqlText);

        expected = "A1  | B1  | 3 | 4 | 5 | 6 |\n" +
                "----------------------------\n" +
                "  1  |  1  | 0 | 0 | 6 | 1 |\n" +
                "  1  |  2  | 0 | 0 | 6 | 2 |\n" +
                "  1  |NULL | 0 | 0 | 6 | 3 |\n" +
                "  1  |NULL | 0 | 1 |18 | 4 |\n" +
                "  2  |  1  | 0 | 0 | 6 | 1 |\n" +
                "  2  |  2  | 0 | 0 | 6 | 2 |\n" +
                "  2  |NULL | 0 | 1 |12 | 3 |\n" +
                "  3  |  1  | 0 | 0 | 6 | 1 |\n" +
                "  3  |  2  | 0 | 0 | 6 | 2 |\n" +
                "  3  |NULL | 0 | 1 |12 | 3 |\n" +
                "NULL |NULL | 1 | 1 |42 | 1 |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testGroupingFunctionInOverClauseOrderBy() throws Exception {
        String sqlText = format("select a1, b1, grouping(a1), grouping(b1), row_number() over (partition by a1 order by grouping(a1)+grouping(b1),b1) from t1 --splice-properties useSpark=%s\n" +
                "group by rollup(a1,b1) order by 1,5", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "A1  | B1  | 3 | 4 | 5 |\n" +
                "------------------------\n" +
                "  1  |  1  | 0 | 0 | 1 |\n" +
                "  1  |  2  | 0 | 0 | 2 |\n" +
                "  1  |NULL | 0 | 0 | 3 |\n" +
                "  1  |NULL | 0 | 1 | 4 |\n" +
                "  2  |  1  | 0 | 0 | 1 |\n" +
                "  2  |  2  | 0 | 0 | 2 |\n" +
                "  2  |NULL | 0 | 1 | 3 |\n" +
                "  3  |  1  | 0 | 0 | 1 |\n" +
                "  3  |  2  | 0 | 0 | 2 |\n" +
                "  3  |NULL | 0 | 1 | 3 |\n" +
                "NULL |NULL | 1 | 1 | 1 |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    @Test
    public void testGroupingFunctionInOverCalusePartitionBy() throws Exception {
        String sqlText = format("select grouping(a1)+grouping(b1), a1, b1, grouping(a1), grouping(b1), count(*), row_number() over (partition by grouping(a1)+grouping(b1) order by a1, b1) from t1 --splice-properties useSpark=%s\n" +
                "group by rollup(a1,b1) order by 1,7", this.useSparkString);

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String expected = "1 | A1  | B1  | 4 | 5 | 6 | 7 |\n" +
                "--------------------------------\n" +
                " 0 |  1  |  1  | 0 | 0 | 6 | 1 |\n" +
                " 0 |  1  |  2  | 0 | 0 | 6 | 2 |\n" +
                " 0 |  1  |NULL | 0 | 0 | 6 | 3 |\n" +
                " 0 |  2  |  1  | 0 | 0 | 6 | 4 |\n" +
                " 0 |  2  |  2  | 0 | 0 | 6 | 5 |\n" +
                " 0 |  3  |  1  | 0 | 0 | 6 | 6 |\n" +
                " 0 |  3  |  2  | 0 | 0 | 6 | 7 |\n" +
                " 1 |  1  |NULL | 0 | 1 |18 | 1 |\n" +
                " 1 |  2  |NULL | 0 | 1 |12 | 2 |\n" +
                " 1 |  3  |NULL | 0 | 1 |12 | 3 |\n" +
                " 2 |NULL |NULL | 1 | 1 |42 | 1 |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();

        sqlText = format("select grouping(a1)+grouping(b1), a1, b1, grouping(a1), grouping(b1), count(*), row_number() over (partition by case when grouping(a1)+grouping(b1)=0 then a1 end order by count(*),a1,b1) from t1 --splice-properties useSpark=%s\n" +
                "group by rollup(a1,b1) order by 1,2,3,7", this.useSparkString);

        rs = methodWatcher.executeQuery(sqlText);

        expected = "1 | A1  | B1  | 4 | 5 | 6 | 7 |\n" +
                "--------------------------------\n" +
                " 0 |  1  |  1  | 0 | 0 | 6 | 1 |\n" +
                " 0 |  1  |  2  | 0 | 0 | 6 | 2 |\n" +
                " 0 |  1  |NULL | 0 | 0 | 6 | 3 |\n" +
                " 0 |  2  |  1  | 0 | 0 | 6 | 1 |\n" +
                " 0 |  2  |  2  | 0 | 0 | 6 | 2 |\n" +
                " 0 |  3  |  1  | 0 | 0 | 6 | 1 |\n" +
                " 0 |  3  |  2  | 0 | 0 | 6 | 2 |\n" +
                " 1 |  1  |NULL | 0 | 1 |18 | 3 |\n" +
                " 1 |  2  |NULL | 0 | 1 |12 | 1 |\n" +
                " 1 |  3  |NULL | 0 | 1 |12 | 2 |\n" +
                " 2 |NULL |NULL | 1 | 1 |42 | 4 |";

        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs));
        rs.close();
    }

    /** negative test cases*/
    @Test
    public void testGroupingFunctionForQueryWithoutRollup() throws Exception {
        String sqlText = "select a1, b1, grouping(a1), count(*) from t1 group by a1,b1";
        try {
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLException e) {
            Assert.assertEquals(e.getSQLState(), SQLState.LANG_FUNCTION_NOT_ALLOWED);
        }
    }

    @Test
    public void testGroupingFunctionInGroupBy() throws Exception {
        String sqlText = "select a1, grouping(a1), count(*) from t1 group by a1,grouping(b1)";
        try {
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLException e) {
            Assert.assertEquals(e.getSQLState(), SQLState.LANG_GROUPING_FUNCTION_CONTEXT_ERROR);
        }
    }

    @Test
    public void testGroupingFunctionInWhereClause() throws Exception {
        String sqlText = "select a1, b1, grouping(a1), count(*) from t1 where grouping(a1) = 1 group by rollup(a1,b1)";
        try {
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLException e) {
            Assert.assertEquals(e.getSQLState(), SQLState.LANG_GROUPING_FUNCTION_CONTEXT_ERROR);
        }
    }

    @Test
    public void testGroupingFunctionInOnClause() throws Exception {
        String sqlText = "select a1, b1, grouping(a1), count(*) from t1 left join t2 on a1=a2 and grouping(a1)=1 group by rollup(a1,b1)";
        try {
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLException e) {
            Assert.assertEquals(e.getSQLState(), SQLState.LANG_GROUPING_FUNCTION_CONTEXT_ERROR);
        }
    }

    @Test
    public void testGroupingFunctionInAggregate() throws Exception {
        String sqlText = "select a1, b1, grouping(a1), count(grouping(b1)) from t1 group by rollup(a1,b1)";
        try {
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLException e) {
            Assert.assertEquals(e.getSQLState(), SQLState.LANG_GROUPING_FUNCTION_CONTEXT_ERROR);
        }
    }

    @Test
    public void testGroupingFunctionInWindowFunction() throws Exception {
        String sqlText = "select a1, b1, row_number() over (partition by grouping(a1) order by b1) from t1 group by a1,b1";
        try {
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLException e) {
            Assert.assertEquals(e.getSQLState(), SQLState.LANG_FUNCTION_NOT_ALLOWED);
        }
    }

    @Test
    public void testGroupingFunctionInSelectWithGroupBy() throws Exception {
        String sqlText = "select a1, b1, grouping(a1) from t1";
        try {
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query is expected to fail with syntax error!");
        } catch (SQLException e) {
            Assert.assertEquals(e.getSQLState(), SQLState.LANG_GROUPING_FUNCTION_CONTEXT_ERROR);
        }
    }
}
