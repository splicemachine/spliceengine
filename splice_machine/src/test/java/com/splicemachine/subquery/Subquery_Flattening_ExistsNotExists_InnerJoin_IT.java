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

package com.splicemachine.subquery;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Collection;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.splicemachine.subquery.SubqueryITUtil.ZERO_SUBQUERY_NODES;
import static com.splicemachine.subquery.SubqueryITUtil.countSubqueriesInPlan;
import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by yxia on 3/5/19.
 */
@RunWith(Parameterized.class)
public class Subquery_Flattening_ExistsNotExists_InnerJoin_IT extends SpliceUnitTest {
    private Boolean useSpark;
    private String joinStrategy;

    private HashMap<String, String> innerJoinType = new HashMap<>();
    private HashMap<String, String> antiJoinType = new HashMap<>();

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(8);
        params.add(new Object[]{true, "NESTEDLOOP"});
        params.add(new Object[]{true, "BROADCAST"});
        params.add(new Object[]{true, "SORTMERGE"});
        params.add(new Object[]{true, "MERGE"});
        params.add(new Object[]{false, "NESTEDLOOP"});
        params.add(new Object[]{false, "BROADCAST"});
        params.add(new Object[]{false, "SORTMERGE"});
        params.add(new Object[]{false, "MERGE"});
        return params;
    }

    public Subquery_Flattening_ExistsNotExists_InnerJoin_IT(boolean useSpark, String joinStrategy) {
        this.joinStrategy = joinStrategy;
        this.useSpark = useSpark;

        innerJoinType.put("NESTEDLOOP", "NestedLoopJoin");
        innerJoinType.put("BROADCAST", "BroadcastJoin");
        innerJoinType.put("SORTMERGE", "MergeSortJoin");
        innerJoinType.put("MERGE", "MergeJoin");

        antiJoinType.put("NESTEDLOOP", "NestedLoopAntiJoin");
        antiJoinType.put("BROADCAST", "BroadcastAntiJoin");
        antiJoinType.put("SORTMERGE", "MergeSortAntiJoin");
        antiJoinType.put("MERGE", "MergeAntiJoin");
    }

    private static final String SCHEMA = Subquery_Flattening_ExistsNotExists_InnerJoin_IT.class.getSimpleName();

    @ClassRule
    public static SpliceSchemaWatcher schemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @ClassRule
    public static SpliceWatcher classWatcher = new SpliceWatcher(SCHEMA);

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table  t1(a1 int, b1 int, c1 int, primary key (a1))")
                .withInsert("insert into t1 values(?, ?, ?)")
                .withRows(rows(
                        row(1, 1, 1),
                        row(2, 2, 2),
                        row(3, 3, 3),
                        row(4, null, 4)))
                .withIndex("create index idx_t1 on t1(b1,a1)")
                .create();

        new TableCreator(conn)
                .withCreate("create table t2 (a2 int, b2 int, c2 int, primary key (a2))")
                .withInsert("insert into t2 values(?, ?, ?)")
                .withRows(rows(
                        row(1, 1, 1),
                        row(2, 2, 2),
                        row(3, 2, 2),
                        row(4, 4, 4),
                        row(5, 5, 5),
                        row(6, null, null)))
                .withIndex("create index idx_t2 on t2(b2, a2)")
                .create();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(classWatcher.getOrCreateConnection(), schemaWatcher.toString());
    }

    @Test
    public void testCorrelatedExists() throws Exception {
        String sql = format("select a1, b1 from t1 " +
                "where exists (select 1 from t2 --splice-properties joinStrategy=%s, useSpark=%b\n" +
                "where b1=b2 and (a2 <0 or a2>0))", this.joinStrategy, this.useSpark);

        String expected = "A1 |B1 |\n" +
                "--------\n" +
                " 1 | 1 |\n" +
                " 2 | 2 |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = methodWatcher.executeQuery("explain " + sql);
        String explainPlanText = TestUtils.FormattedResult.ResultFactory.toString(rs);
        assertEquals(ZERO_SUBQUERY_NODES, countSubqueriesInPlan(explainPlanText));
        assertTrue(format("Expected join strategy %s not found in the explain %s", innerJoinType.get(joinStrategy), explainPlanText),
                explainPlanText.contains(innerJoinType.get(joinStrategy)));
    }

    @Test
    public void testCorrelatedNotExistsWithOrPredicate() throws Exception {
        String sql = format("select a1, b1 from t1 " +
                "where not exists (select 1 from t2 --splice-properties joinStrategy=%s, useSpark=%b\n" +
                "where b1=b2 and (a2 <0 or a2>0))", this.joinStrategy, this.useSpark);

        String expected = "A1 | B1  |\n" +
                "----------\n" +
                " 3 |  3  |\n" +
                " 4 |NULL |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = methodWatcher.executeQuery("explain " + sql);
        String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(ZERO_SUBQUERY_NODES, countSubqueriesInPlan(explainPlanText));
        assertTrue(format("Expected join strategy %s not found in the explain %s", antiJoinType.get(joinStrategy), explainPlanText),
                explainPlanText.contains(antiJoinType.get(joinStrategy)));
    }

    @Test
    public void testCorrelatedNotExistsWithNotEqual() throws Exception {
        String sql = format("select a1, b1 from t1 " +
                "where not exists (select 1 from t2 --splice-properties joinStrategy=%s, useSpark=%b\n" +
                "where b1=b2 and (a2<>0))", this.joinStrategy, this.useSpark);

        String expected = "A1 | B1  |\n" +
                "----------\n" +
                " 3 |  3  |\n" +
                " 4 |NULL |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = methodWatcher.executeQuery("explain " + sql);
        String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(ZERO_SUBQUERY_NODES, countSubqueriesInPlan(explainPlanText));
        assertTrue(format("Expected join strategy %s not found in the explain %s", antiJoinType.get(joinStrategy), explainPlanText),
                explainPlanText.contains(antiJoinType.get(joinStrategy)));
    }

    @Test
    public void testCorrelatedNotExistsWithMoreThanOneTableInExists() throws Exception {
        String sql = format("select a1, b1 from t1 " +
                "where not exists (select 1 from t2 as X, t2 as Y --splice-properties joinStrategy=%s, useSpark=%b\n" +
                "where b1=X.b2 and X.b2 = Y.b2 and (X.a2<>0))", this.joinStrategy, this.useSpark);

        String expected = "A1 | B1  |\n" +
                "----------\n" +
                " 3 |  3  |\n" +
                " 4 |NULL |";
        ResultSet rs = methodWatcher.executeQuery(sql);
        assertEquals(expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        rs = methodWatcher.executeQuery("explain " + sql);
        String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        assertEquals(ZERO_SUBQUERY_NODES, countSubqueriesInPlan(explainPlanText));
        Pattern pattern = Pattern.compile("(.*)LeftOuterJoin(.*)", Pattern.MULTILINE | Pattern.CASE_INSENSITIVE);
        Matcher m = pattern.matcher(explainPlanText);

        assertTrue(format("Expected left join not found in the explain %s", explainPlanText),
                m.find());

    }
    

}
