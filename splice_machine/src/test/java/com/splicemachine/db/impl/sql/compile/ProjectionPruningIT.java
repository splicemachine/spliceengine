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
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test.LongerThanTwoMinutes;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test_tools.TableCreator;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Created by yxia on 4/23/18.
 */
/* The test result is dependent on different values of the service property setting at the time of the run, so
   need to run sequentially
 */
@Category({SerialTest.class, LongerThanTwoMinutes.class})
@RunWith(Parameterized.class)
public class ProjectionPruningIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(ProjectionPruningIT.class);
    public static final String CLASS_NAME = ProjectionPruningIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"true"});
        params.add(new Object[]{"false"});
        return params;
    }

    private String projectionPruningDisabled;
    private static String savedPropertyValue;

    public ProjectionPruningIT(String propertyDisabled) {
        projectionPruningDisabled = propertyDisabled;
    }

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1(a1 int, b1 varchar(5), c1 date, d1 float)")
                .withIndex("create index idx_t1 on t1(b1)")
                .withInsert("insert into t1 values(?,?,?,?)")
                .withRows(rows(
                        row(1, "aaa", "2018-04-18", 1.0),
                        row(2, "bbb", "2018-04-17", 2.0),
                        row(3, "ccc", "2018-04-16", 3.0)))
                .create();


        new TableCreator(conn)
                .withCreate("create table t2 (a2 int, b2 varchar(5), c2 date, d2 float)")
                .create();

        new TableCreator(conn)
                .withCreate("create table t3 (a3 int, b3 int, c3 int, d3 int)")
                .withInsert("insert into t3 values(?,?,?,?)")
                .withRows(rows(
                        row(1, 1, 1, 1),
                        row(1, 2, 2, 2),
                        row(1, 2, 3, 3),
                        row(2, 4, 4, 4),
                        row(2, 5, 5, 5),
                        row(2, 5, 6, 6)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t4 (a4 int, b4 varchar(5), c4 date, d4 float)")
                .withInsert("insert into t4 values(?,?,?,?)")
                .withRows(rows(
                        row(1, "aaa", "2018-04-18", 1.0),
                        row(2, "bbb", "2018-04-17", 2.0),
                        row(3, "ccc", "2018-04-16", 3.0)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t5 (a5 int, b5 int, c5 int, d5 int)")
                .withIndex("create index idx_t5 on t5(a5, b5, d5)")
                .withInsert("insert into t5 values(?,?,?,?)")
                .withRows(rows(
                        row(1, 1, 1, 1),
                        row(1, 2, 2, 2),
                        row(1, 2, 3, 3),
                        row(2, 4, 4, 4),
                        row(2, 5, 5, 5),
                        row(2, 5, 6, 6)))
                .create();

        new TableCreator(conn)
                .withCreate("create table target (a6 int, b6 varchar(5), c6 date, d6 float)")
                .withIndex("create index idx_target on target(b6, a6)")
                .withInsert("insert into target values(?,?,?,?)")
                .withRows(rows(
                        row(11, "xxx", "2018-04-22", 1.0),
                        row(12, "yyy", "2018-04-23", 2.0),
                        row(13, "zzz", "2018-04-24", 3.0)))
                .create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());

        //save the original value of the projectionPruningDisabled property
        ResultSet rs = spliceClassWatcher.executeQuery("values syscs_util.syscs_get_database_property('derby.database.projectionPruningDisabled')");
        savedPropertyValue = null;
        if (rs.next())
            savedPropertyValue = rs.getString(1);
        rs.close();
    }

    @AfterClass
    public static void restoreProperty() throws Exception {
        if (savedPropertyValue == null || savedPropertyValue.equals("NULL"))
            spliceClassWatcher.execute("call syscs_util.syscs_set_global_database_property('derby.database.projectionPruningDisabled', NULL)");
        else
            spliceClassWatcher.execute(format("call syscs_util.syscs_set_global_database_property('derby.database.projectionPruningDisabled', '%s')", savedPropertyValue));
    }

    private void assertPruneResult(String query,
                                  String expected,
                                  boolean indexShouldBePicked) throws Exception {
        ResultSet rs = methodWatcher.executeQuery(query);
        assertEquals("\n" + query + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
        ResultSet rs2 = methodWatcher.executeQuery("explain " + query);
        String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs2);
        boolean coveringIndexScanPresent = explainPlanText.contains("IndexScan") && !explainPlanText.contains("IndexLookup");
        if (indexShouldBePicked)
            Assert.assertTrue("Query is expected to pick covering index, the current plan is: " + explainPlanText, coveringIndexScanPresent);
        else
            Assert.assertTrue("Query is not expected to pick covering index, the current plan is: " + explainPlanText, !coveringIndexScanPresent);

        rs2.close();
    }

    private void setProjectionPruningProperty(String projectionPruningDisabled) throws Exception {
        methodWatcher.execute(format("call syscs_util.syscs_set_global_database_property('derby.database.projectionPruningDisabled', '%s')", projectionPruningDisabled));
        methodWatcher.execute("call syscs_util.syscs_empty_statement_cache()");
    }

    @Test
    public void testProjectPruningWithDT() throws Exception {
        setProjectionPruningProperty(projectionPruningDisabled);

        /* case 1: index should be picked up */
        String sqlText = "select b1 from (select * from t1 --splice-properties index=idx_t1\n) dt where b1='ccc'";

        String expected = "B1  |\n" +
                "-----\n" +
                "ccc |";

        assertPruneResult(sqlText, expected, projectionPruningDisabled.equals("false")?true:false);
        /* case 1-1: index shouldn't be picked up */
        sqlText = "select b1 from (select * from t1) dt where b1='ccc' and a1=3";
        expected = "B1  |\n" +
                "-----\n" +
                "ccc |";

        assertPruneResult(sqlText, expected, false);

        /* case 2: dt project no column out */
        sqlText = "select 'a' from (select 1 from t1  --splice-properties index=idx_t1\n {limit 2}) dt";
        expected = "1 |\n" +
                "----\n" +
                " a |\n" +
                " a |";

        assertPruneResult(sqlText, expected, true);

        /* case 3: expression in the projection list not projected out */
        sqlText = "select b1 from (select b1, a1+d1 as X, d1 from t1 --splice-properties index=idx_t1\n order by b1) dt";
        expected = "B1  |\n" +
                "-----\n" +
                "aaa |\n" +
                "bbb |\n" +
                "ccc |";

        assertPruneResult(sqlText, expected, projectionPruningDisabled.equals("false")?true:false);

        /* case 3-1: expression in the projection list projected out */
        sqlText = "select X from (select a1, a1+d1 as X, d1 from t1 order by b1) dt";
        expected = "X  |\n" +
                "-----\n" +
                "2.0 |\n" +
                "4.0 |\n" +
                "6.0 |";

        assertPruneResult(sqlText, expected, false);

        /* case 4: non-covering index in dt */
        sqlText = "select * from t3 where a3 in (select a1 from t1 --splice-properties index=idx_t1\n" +
                "                              where b1 > 'aaa')";
        expected = "A3 |B3 |C3 |D3 |\n" +
                "----------------\n" +
                " 2 | 4 | 4 | 4 |\n" +
                " 2 | 5 | 5 | 5 |\n" +
                " 2 | 5 | 6 | 6 |";

        assertPruneResult(sqlText, expected, false);

        /* case 4-1: non-covering index in dt */
        sqlText = "select * from t3 where a3 in (select a1 from\n" +
                "                               (select * from t1 --splice-properties index=idx_t1\n" +
                "                                order by a1\n" +
                "                               ) dt where dt.b1 > 'aaa')";
        expected = "A3 |B3 |C3 |D3 |\n" +
                "----------------\n" +
                " 2 | 4 | 4 | 4 |\n" +
                " 2 | 5 | 5 | 5 |\n" +
                " 2 | 5 | 6 | 6 |";

        assertPruneResult(sqlText, expected, false);
    }


    @Test
    public void testDerivedTableWithOrderByAndLimitN() throws Exception {
        setProjectionPruningProperty(projectionPruningDisabled);

        /* case 1: index should not be picked up */
        String sqlText = "select b1 from (select b1 from t1 order by d1 desc {limit 2}) dt";

        String expected = "B1  |\n" +
                "-----\n" +
                "bbb |\n" +
                "ccc |";

        assertPruneResult(sqlText, expected, false);

        /* case 1-1: index should be picked up */
        sqlText = "select b1 from (select * from t1  --splice-properties index=idx_t1\n order by b1 desc {limit 2}) dt";

        expected = "B1  |\n" +
                "-----\n" +
                "bbb |\n" +
                "ccc |";

        assertPruneResult(sqlText, expected, projectionPruningDisabled.equals("false")?true:false);

        /* case 2: orderby by a constant */
        sqlText = "select b1 from (select * from t1 --splice-properties index=idx_t1\n" +
                "order by 'xxxx' {limit 2}) dt";

        expected = "B1  |\n" +
                "-----\n" +
                "aaa |\n" +
                "bbb |";

        assertPruneResult(sqlText, expected, projectionPruningDisabled.equals("false")?true:false);
    }

    @Test
    public void testMultipleLevelOfDTs() throws Exception {
        setProjectionPruningProperty(projectionPruningDisabled);

        /* case 1: nested DT */
        String sqlText = "select * from (select b1 from (select * from t1  --splice-properties index=idx_t1\n order by b1 desc {limit 2}) dt) dt1";

        /* Please note, despite "order by b1 in desc order", result is sorted in the function assertPruneResult,
           thus it is in this order */
        String expected = "B1  |\n" +
                "-----\n" +
                "bbb |\n" +
                "ccc |";

        assertPruneResult(sqlText, expected, projectionPruningDisabled.equals("false")?true:false);

        /* case 2: back to back DT */
        sqlText = "select dt1.b1, dt2.b1 from (select * from t1  --splice-properties index=idx_t1\n " +
                "order by b1 desc {limit 2}) dt1, (select a1, b1, c1 from t1 where b1='ccc')dt2 where dt1.b1=dt2.b1";

        expected = "B1  |B1  |\n" +
                "----------\n" +
                "ccc |ccc |";

        assertPruneResult(sqlText, expected, projectionPruningDisabled.equals("false")?true:false);
    }

    @Test
    public void testOuterJoinInDT() throws Exception {
        setProjectionPruningProperty(projectionPruningDisabled);

        /* case 1: DT projects just b1 */
        String sqlText = "select d3, b1 from t3, (select b1, a4 from t1  --splice-properties index=idx_t1\n left join t4 on b1=b4) dt where d3=1";

        String expected = "D3 |B1  |\n" +
                "---------\n" +
                " 1 |aaa |\n" +
                " 1 |bbb |\n" +
                " 1 |ccc |";

        assertPruneResult(sqlText, expected, true);

        /* case 1-1: DT projects all columns */
        sqlText = "select d3, b1, b4 from t3, (select * from t1  --splice-properties index=idx_t1\n left join t4 on b1=b4) dt where d3=1";

        expected = "D3 |B1  |B4  |\n" +
                "--------------\n" +
                " 1 |aaa |aaa |\n" +
                " 1 |bbb |bbb |\n" +
                " 1 |ccc |ccc |";

        assertPruneResult(sqlText, expected, projectionPruningDisabled.equals("false")?true:false);

        /* case 2: right join */
        sqlText = "select d3, b1, a4 from t3, (select * from t1 --splice-properties index=idx_t1\n " +
                "right join t4 on b1=b4 and b1='ccc') dt where d3=1";

        expected = "D3 | B1  |A4 |\n" +
                "--------------\n" +
                " 1 |NULL | 1 |\n" +
                " 1 |NULL | 2 |\n" +
                " 1 | ccc | 3 |";

        assertPruneResult(sqlText, expected, projectionPruningDisabled.equals("false")?true:false);

        /* case 3: multiple outer join */
        sqlText = "select d3, b1, b4, b2 from t3, (select * from t1  --splice-properties index=idx_t1\n left join t4 on b1=b4 left join t2 on b1=b2) dt  where d3=1";

        expected = "D3 |B1  |B4  | B2  |\n" +
                "--------------------\n" +
                " 1 |aaa |aaa |NULL |\n" +
                " 1 |bbb |bbb |NULL |\n" +
                " 1 |ccc |ccc |NULL |";

        assertPruneResult(sqlText, expected, projectionPruningDisabled.equals("false")?true:false);

        /* case 3: DT in outer join */
        sqlText = "select d3, b1, b4, b2 from t3, (select * from (select * from t1  --splice-properties index=idx_t1\n) dt0 left join t4 on b1=b4 left join t2 on b1=b2) dt  where d3=1";

        expected = "D3 |B1  |B4  | B2  |\n" +
                "--------------------\n" +
                " 1 |aaa |aaa |NULL |\n" +
                " 1 |bbb |bbb |NULL |\n" +
                " 1 |ccc |ccc |NULL |";

        assertPruneResult(sqlText, expected, projectionPruningDisabled.equals("false")?true:false);
    }

    @Test
    public void testAggregationInDT() throws Exception {
        setProjectionPruningProperty(projectionPruningDisabled);

        /* case 1: group by column in the select list of the DT */
        String sqlText = "select CC from (select b1, count(*) as CC from t1  --splice-properties index=idx_t1\n group by b1) dt";

        String expected = "CC |\n" +
                "----\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |";

        assertPruneResult(sqlText, expected, true);

        /* case 2: group by column not in the select list of the DT */
        sqlText = "select CC from (select count(*) as CC from t1  --splice-properties index=idx_t1\n group by b1) dt";

        expected = "CC |\n" +
                "----\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |";

        assertPruneResult(sqlText, expected, true);

        /* case 3: scalar aggregation */
        sqlText = "select CC from (select count(*) as CC from t1  --splice-properties index=idx_t1\n) dt";

        expected = "CC |\n" +
                "----\n" +
                " 3 |";

        assertPruneResult(sqlText, expected, true);

        /* case 4: expression in aggregation */
        sqlText = "select CC from (select b1, count(a1+d1) as CC from t1 group by b1) dt";

        expected = "CC |\n" +
                "----\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |";

        assertPruneResult(sqlText, expected, false);

        /* case 5: project only the grouping column */
        sqlText = "select b1 from (select b1, count(*) as CC from t1  --splice-properties index=idx_t1\n group by b1) dt";

        expected = "B1  |\n" +
                "-----\n" +
                "aaa |\n" +
                "bbb |\n" +
                "ccc |";

        assertPruneResult(sqlText, expected, true);

        /* case 6: aggregation in expression */
        sqlText = "select b1 from (select b1, count(*)+max(a1) as CC from t1 group by b1) dt";

        expected = "B1  |\n" +
                "-----\n" +
                "aaa |\n" +
                "bbb |\n" +
                "ccc |";

        assertPruneResult(sqlText, expected, false);

        /* case 7: aggregate with having, this should not pick index scan due to c1 referenced */
        sqlText = "select b1 from (select b1 from t1 group by b1 having count(c1)=1) dt";

        expected = "B1  |\n" +
                "-----\n" +
                "aaa |\n" +
                "bbb |\n" +
                "ccc |";

        assertPruneResult(sqlText, expected, false);
    }

    @Test
    public void testWindowFunctionInDT() throws Exception {
        setProjectionPruningProperty(projectionPruningDisabled);

        /* case 1 */
        String sqlText = "select a5 from (select a5, sum(d5) over (partition by b5 order by a5) from (select * from t5  --splice-properties index=idx_t5\n ) dt0 ) dt";

        String expected = "A5 |\n" +
                "----\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 2 |\n" +
                " 2 |\n" +
                " 2 |";

        assertPruneResult(sqlText, expected, projectionPruningDisabled.equals("false")?true:false);

        /* case 2 */
        sqlText = "select a5 from (select a5, first_value(d5) over (partition by b5 order by a5) as X from (select * from t5  --splice-properties index=idx_t5\n) dt0 ) dt";

        expected = "A5 |\n" +
                "----\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 2 |\n" +
                " 2 |\n" +
                " 2 |";

        assertPruneResult(sqlText, expected, projectionPruningDisabled.equals("false")?true:false);

        /* case 3 */
        sqlText = "select X from (select a5, first_value(d5) over (partition by b5 order by d5) as X from (select * from t5  --splice-properties index=idx_t5\n) dt0 ) dt";

        expected = "X |\n" +
                "----\n" +
                " 1 |\n" +
                " 2 |\n" +
                " 2 |\n" +
                " 4 |\n" +
                " 5 |\n" +
                " 5 |";

        assertPruneResult(sqlText, expected, projectionPruningDisabled.equals("false")?true:false);
    }

    @Test
    public void testDistinctInDT() throws Exception {
        setProjectionPruningProperty(projectionPruningDisabled);

        /* case 1: group by column in the select list of the DT */
        String sqlText = "select count(*) from (select distinct a3 from t3) dt";

        String expected = "1 |\n" +
                "----\n" +
                " 2 |";

        assertPruneResult(sqlText, expected, false);

        /* case 2: group by to distinct conversion in DT */
        sqlText = "select count(*) from (select 1 from t3 group by a3) dt";

        expected = "1 |\n" +
                "----\n" +
                " 2 |";

        assertPruneResult(sqlText, expected, false);

        /* case 2-1: group by to distinct conversion in DT */
        sqlText = "select count(*) from (select 1 from t3 group by b3) dt";

        expected = "1 |\n" +
                "----\n" +
                " 4 |";

        assertPruneResult(sqlText, expected, false);
    }

    @Test
    public void testSetOperation() throws Exception {
        setProjectionPruningProperty(projectionPruningDisabled);

        /* case 1: DT in set operation  */
        String sqlText = "select b1 from (select * from t1  --splice-properties index=idx_t1\n ) dt union all select b2 from t2";

        String expected = "B1  |\n" +
                "-----\n" +
                "aaa |\n" +
                "bbb |\n" +
                "ccc |";

        assertPruneResult(sqlText, expected, projectionPruningDisabled.equals("false"));

        /* case 2: set in a subquery */
        sqlText = "select b1 from t1 where a1 in (select a3 from t3 union all values 4,5,6)";

        expected = "B1  |\n" +
                "-----\n" +
                "aaa |\n" +
                "bbb |";

        assertPruneResult(sqlText, expected, false);

        /* case 2-1: set operation in a subquery, index scan can be picked */
        sqlText = "select b3 from t3 where 'aaa' in (select b1 from (select * from (select * from t1  --splice-properties index=idx_t1\n ) dt0) dt union all values 'yyy')";

        expected = "B3 |\n" +
                "----\n" +
                " 1 |\n" +
                " 2 |\n" +
                " 2 |\n" +
                " 4 |\n" +
                " 5 |\n" +
                " 5 |";

        assertPruneResult(sqlText, expected, projectionPruningDisabled.equals("false")?true:false);

        /* case 3: set operation with predicate */
        sqlText = "select b1 from (select * from t1) dt where a1>1 union all select b2 from t2";

        expected = "B1  |\n" +
                "-----\n" +
                "bbb |\n" +
                "ccc |";

        assertPruneResult(sqlText, expected,false);

        /* case 4: set operation in DT whose outer block does not reference any particular column from the SET operation */
        sqlText = "select count(*) from (select a1 from t1 intersect select a3 from t3) dt";

        expected = "1 |\n" +
                "----\n" +
                " 2 |";

        assertPruneResult(sqlText, expected, false);

        /* case 5: order by in set operation */
        sqlText = "select b1 from (select * from t1  --splice-properties index=idx_t1\n ) dt union all select b2 from t2 order by 1";

        expected = "B1  |\n" +
                "-----\n" +
                "aaa |\n" +
                "bbb |\n" +
                "ccc |";

        assertPruneResult(sqlText, expected, projectionPruningDisabled.equals("false")?true:false);
    }

    @Test
    public void testSubquery() throws Exception {
        setProjectionPruningProperty(projectionPruningDisabled);

        /* case 1: view in subquery, we should see index scan being picked up  */
        String sqlText = "select b3 from t3 where 'bbb' in (select b1 from (select * from t1 --splice-properties index=idx_t1\n) v1)";

        String expected = "B3 |\n" +
                "----\n" +
                " 1 |\n" +
                " 2 |\n" +
                " 2 |\n" +
                " 4 |\n" +
                " 5 |\n" +
                " 5 |";

        assertPruneResult(sqlText, expected, projectionPruningDisabled.equals("false")?true:false);

        /* case 2: correlated case, covering index should be picked up for both outer table and subquery table */
        sqlText = "select b1 from t1 --splice-properties index=idx_t1\n " +
                "where 'bbb' in (select b1 from (select * from t1 --splice-properties index=idx_t1\n) v1 where t1.b1=v1.b1)";

        expected = "B1  |\n" +
                "-----\n" +
                "bbb |";

        assertPruneResult(sqlText, expected, projectionPruningDisabled.equals("false")?true:false);

        /* case 3: correlated case, covering index scan should not be picked up for subquery table due to order by referenced a1 */
        sqlText = "select b1 from t1 --splice-properties index=idx_t1\n " +
                "where 'bbb' in (select b1 from (select * from t1 --splice-properties index=idx_t1\n) v1 where t1.b1=v1.b1 order by a1)";

        expected = "B1  |\n" +
                "-----\n" +
                "bbb |";

        assertPruneResult(sqlText, expected, false);

        /* case 4: subquery in having clause, correlation cannot be aggregate, but just groupby columns */
        sqlText = "select a1, count(*) from t1 group by a1 having a1 in (select a2 from t2 where a1=a2)";

        expected = "";

        assertPruneResult(sqlText, expected, false);

        /* case 5: scalar subquery in the where clause */
        sqlText = "select a1 from t1 where d1 between (select distinct a3 from t3 where a3=1) and (select distinct a3+3 from t3 where a3=1)";

        expected = "A1 |\n" +
                "----\n" +
                " 1 |\n" +
                " 2 |\n" +
                " 3 |";

        assertPruneResult(sqlText, expected, false);

        /* case 6: correlated reference is the only reference from the table */
        sqlText = "select 1 from (select * from t1 --splice-properties index=idx_t1\n where exists (select a4 from t4 where b1=b4)) dt";

        expected = "1 |\n" +
                "----\n" +
                " 1 |\n" +
                " 1 |\n" +
                " 1 |";

        assertPruneResult(sqlText, expected, projectionPruningDisabled.equals("false")?true:false);

        /* case 7: not in subquery */
        sqlText = "select b1 from t1 --splice-properties index=idx_t1\n where b1 not in (select b4 from t4 where a1=a4)";

        expected = "";

        assertPruneResult(sqlText, expected, false);

        /* case 8: check correlated exists subquery */
        sqlText = "select b1 from t1 --splice-properties index=idx_t1\n" +
                "where not exists (select b4, c4 from t4 where b1=b4)";

        expected = "";

        assertPruneResult(sqlText, expected, true);

        /* case 8-1: non-covering index */
        sqlText = "select b1 from t1 --splice-properties index=idx_t1\n" +
                "where exists (select b4, c4 from t4 where a1=a4)";

        expected = "B1  |\n" +
                "-----\n" +
                "aaa |\n" +
                "bbb |\n" +
                "ccc |";

        assertPruneResult(sqlText, expected, false);

        /* case 8-2: non-covering index, currently we don't have the optimization to prune the select list of exists subquery,
         * so index lookup is still needed */
        sqlText = "select b4 from t4\n" +
                "where exists (select b1, c1 from t1 --splice-properties index=idx_t1\n" +
                " where b1=b4)";

        expected = "B4  |\n" +
                "-----\n" +
                "aaa |\n" +
                "bbb |\n" +
                "ccc |";

        assertPruneResult(sqlText, expected, false);

        /* case 9: subquery in the select clause */
        sqlText = "select (select d4 from t4 where b1=b4) as XX from t1 --splice-properties index=idx_t1\n";

        expected = "XX  |\n" +
                "-----\n" +
                "1.0 |\n" +
                "2.0 |\n" +
                "3.0 |";

        assertPruneResult(sqlText, expected, true);
    }


    @Test
    public void testDB7052() throws Exception {
        setProjectionPruningProperty(projectionPruningDisabled);

        String sqlText = "select a1, a2, a4, a5 from t1 left join (select distinct a2, b2 from t2 group by a2, b2) dt --splice-properties useSpark=true, joinStrategy=nestedloop\n" +
                "on a1=a2\n" +
                "left join t4 --splice-properties joinStrategy=sortmerge\n" +
                "on a1=a4\n" +
                "left join t5  --splice-properties joinStrategy=broadcast, useDefaultRowCount=100000000\n" +
                "on a1=a5";

        String expected = "A1 | A2  |A4 | A5  |\n" +
                "--------------------\n" +
                " 1 |NULL | 1 |  1  |\n" +
                " 1 |NULL | 1 |  1  |\n" +
                " 1 |NULL | 1 |  1  |\n" +
                " 2 |NULL | 2 |  2  |\n" +
                " 2 |NULL | 2 |  2  |\n" +
                " 2 |NULL | 2 |  2  |\n" +
                " 3 |NULL | 3 |NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testDB7160() throws Exception {
        setProjectionPruningProperty(projectionPruningDisabled);

        String sqlText = "SELECT count(*) FROM t3 z1 WHERE EXISTS\n" +
                "( SELECT 1 FROM ( SELECT b3 FROM t3 z2 WHERE EXISTS ( SELECT b3 FROM t3 z3 WHERE EXISTS\n" +
                "(select 1 from --splice-properties joinOrder=fixed\n" +
                "          t3 y1 --splice-properties joinStrategy=SORTMERGE\n" +
                "          inner join t3 y2 --splice-properties joinStrategy=SORTMERGE\n" +
                "          on y1.a3=y2.a3 inner join t3 y3 --splice-properties joinStrategy=SORTMERGE\n" +
                "          on y2.b3 = y3.b3 inner join t3 y4 on y2.c3 = y4.c3)))mytab)\n";

        String expected = "1 |\n" +
                "----\n" +
                " 6 |";

        ResultSet rs = null;
        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testDTInFlattenableInnerJoinFormat() throws Exception {
        setProjectionPruningProperty(projectionPruningDisabled);

        String sqlText = "with temp1 as (select * from (select * from t1) TX INNER JOIN t4 on b1=b4)\n" +
                "select count(1) from temp1";

        String expected = "1 |\n" +
                "----\n" +
                " 3 |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testInsertSelect() throws Exception {
        setProjectionPruningProperty(projectionPruningDisabled);

        TestConnection conn = methodWatcher.getOrCreateConnection();
        boolean oldAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try (Statement statement = conn.createStatement()) {
            /* case 1, covering index */
            String sqlText = "insert into target(b6) select b1 from (select * from t1 --splice-properties index=idx_t1\n) dt where b1='bbb'";
            /* check plan */
            ResultSet rs = statement.executeQuery("explain " + sqlText);

            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            boolean coveringIndexScanPresent = explainPlanText.contains("IndexScan") && !explainPlanText.contains("IndexLookup");
            if (projectionPruningDisabled.equals("false"))
                Assert.assertTrue("Query is expected to pick covering index, the current plan is: " + explainPlanText, coveringIndexScanPresent);
            else
                Assert.assertTrue("Query is not expected to pick covering index, the current plan is: " + explainPlanText, !coveringIndexScanPresent);

            rs.close();

            /* perform update */
            statement.executeUpdate(sqlText);

            /* case 2, non-covering index */
            sqlText = "insert into target(b6, c6) select b1,c1 from (select * from t1 --splice-properties index=idx_t1\n) dt where b1='ccc'";

            rs = statement.executeQuery("explain " + sqlText);

            explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            coveringIndexScanPresent = explainPlanText.contains("IndexScan") && !explainPlanText.contains("IndexLookup");
            Assert.assertTrue("Query is not expected to pick covering index, the current plan is: " + explainPlanText, !coveringIndexScanPresent);
            rs.close();

            /* perform update */
            statement.executeUpdate(sqlText);

            /* check result */
            String expected = "A6  |B6  |    C6     | D6  |\n" +
                    "-----------------------------\n" +
                    " 11  |xxx |2018-04-22 | 1.0 |\n" +
                    " 12  |yyy |2018-04-23 | 2.0 |\n" +
                    " 13  |zzz |2018-04-24 | 3.0 |\n" +
                    "NULL |bbb |   NULL    |NULL |\n" +
                    "NULL |ccc |2018-04-16 |NULL |";
            sqlText = "select * from target";
            rs = statement.executeQuery(sqlText);
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();

        } finally {
            conn.rollback();
            conn.setAutoCommit(oldAutoCommit);
        }
    }

    @Test
    public void testDelete() throws Exception {
        setProjectionPruningProperty(projectionPruningDisabled);

        TestConnection conn = methodWatcher.getOrCreateConnection();
        boolean oldAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try (Statement statement = conn.createStatement()) {
            /* case 1, covering index */
            String sqlText = "delete from target where exists (select 1 from (select * from t5 --splice-properties index=idx_t5\n) dt where a6=a5+12)";
            /* check plan */
            ResultSet rs = statement.executeQuery("explain " + sqlText);

            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            boolean coveringIndexScanPresent = explainPlanText.contains("IndexScan") && !explainPlanText.contains("IndexLookup");
            if (projectionPruningDisabled.equals("false"))
                Assert.assertTrue("Query is expected to pick covering index, the current plan is: " + explainPlanText, coveringIndexScanPresent);
            else
                Assert.assertTrue("Query is not expected to pick covering index, the current plan is: " + explainPlanText, !coveringIndexScanPresent);

            rs.close();

            /* perform update */
            statement.executeUpdate(sqlText);

            /* case 2, non-covering index */
            sqlText = "delete from target where exists (select 1 from (select * from t5 --splice-properties index=idx_t5\n) dt where c5=2 and a6=a5+11)";

            rs = statement.executeQuery("explain " + sqlText);

            explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            coveringIndexScanPresent = explainPlanText.contains("IndexScan") && !explainPlanText.contains("IndexLookup");
            Assert.assertTrue("Query is not expected to pick covering index, the current plan is: " + explainPlanText, !coveringIndexScanPresent);
            rs.close();

            /* perform update */
            statement.executeUpdate(sqlText);

            /* check result */
            String expected = "A6 |B6  |    C6     |D6  |\n" +
                    "--------------------------\n" +
                    "11 |xxx |2018-04-22 |1.0 |";
            sqlText = "select * from target";
            rs = statement.executeQuery(sqlText);
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();

        } finally {
            conn.rollback();
            conn.setAutoCommit(oldAutoCommit);
        }
    }

    @Test
    public void testUpdate() throws Exception {
        setProjectionPruningProperty(projectionPruningDisabled);

        TestConnection conn = methodWatcher.getOrCreateConnection();
        boolean oldAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try (Statement statement = conn.createStatement()) {
            /* case 1, covering index */
            String sqlText = "update target set a6=a6+100 where a6 in (select a5+10 from (select * from t5 --splice-properties index=idx_t5\n) dt)";
            /* check plan */
            ResultSet rs = statement.executeQuery("explain " + sqlText);

            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            boolean coveringIndexScanPresent = explainPlanText.contains("IndexScan") && !explainPlanText.contains("IndexLookup");
            if (projectionPruningDisabled.equals("false"))
                Assert.assertTrue("Query is expected to pick covering index, the current plan is: " + explainPlanText, coveringIndexScanPresent);
            else
                Assert.assertTrue("Query is not expected to pick covering index, the current plan is: " + explainPlanText, !coveringIndexScanPresent);

            rs.close();

            /* perform update */
            statement.executeUpdate(sqlText);

            /* check result */
            String expected = "A6  |B6  |    C6     |D6  |\n" +
                    "---------------------------\n" +
                    "111 |xxx |2018-04-22 |1.0 |\n" +
                    "112 |yyy |2018-04-23 |2.0 |\n" +
                    "13  |zzz |2018-04-24 |3.0 |";
            sqlText = "select * from target";
            rs = statement.executeQuery(sqlText);
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();

            /* case 2, non-covering index */
            sqlText = "update target set a6=a6+1000 where exists (select 1 from (select * from t5 --splice-properties index=idx_t5\n) dt where c5=5 and a6=a5+11)";

            rs = statement.executeQuery("explain " + sqlText);

            explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            coveringIndexScanPresent = explainPlanText.contains("IndexScan") && !explainPlanText.contains("IndexLookup");
            Assert.assertTrue("Query is not expected to pick covering index, the current plan is: " + explainPlanText, !coveringIndexScanPresent);
            rs.close();

            /* perform update */
            statement.executeUpdate(sqlText);

            /* check result */
            expected = "A6  |B6  |    C6     |D6  |\n" +
                    "----------------------------\n" +
                    "1013 |zzz |2018-04-24 |3.0 |\n" +
                    " 111 |xxx |2018-04-22 |1.0 |\n" +
                    " 112 |yyy |2018-04-23 |2.0 |";
            sqlText = "select * from target";
            rs = statement.executeQuery(sqlText);
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();

        } finally {
            conn.rollback();
            conn.setAutoCommit(oldAutoCommit);
        }
    }

    @Test
    public void testUpdateThroughSubquery() throws Exception {
        setProjectionPruningProperty(projectionPruningDisabled);

        TestConnection conn = methodWatcher.getOrCreateConnection();
        boolean oldAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try (Statement statement = conn.createStatement()) {
            /* case 1, covering index */
            String sqlText = "update target set b6=(select b1 from (select * from t1 --splice-properties index=idx_t1\n) dt where b6=case when b1='aaa' then 'xxx' end)";
            /* check plan */
            ResultSet rs = statement.executeQuery("explain " + sqlText);

            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            boolean coveringIndexScanPresent = explainPlanText.contains("IndexScan") && !explainPlanText.contains("IndexLookup");
            if (projectionPruningDisabled.equals("false"))
                Assert.assertTrue("Query is expected to pick covering index, the current plan is: " + explainPlanText, coveringIndexScanPresent);
            else
                Assert.assertTrue("Query is not expected to pick covering index, the current plan is: " + explainPlanText, !coveringIndexScanPresent);

            rs.close();

            /* perform update */
            statement.executeUpdate(sqlText);

            /* check result */
            String expected = "A6 | B6  |    C6     |D6  |\n" +
                    "---------------------------\n" +
                    "11 | aaa |2018-04-22 |1.0 |\n" +
                    "12 |NULL |2018-04-23 |2.0 |\n" +
                    "13 |NULL |2018-04-24 |3.0 |";
            sqlText = "select * from target";
            rs = statement.executeQuery(sqlText);
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();

            /* case 2, non-covering index */
            sqlText = "update target set b6=(select b1 from (select * from t1 --splice-properties index=idx_t1\n) dt where a6=a1+10 and a1=3)";

            rs = statement.executeQuery("explain " + sqlText);

            explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            coveringIndexScanPresent = explainPlanText.contains("IndexScan") && !explainPlanText.contains("IndexLookup");
            Assert.assertTrue("Query is not expected to pick covering index, the current plan is: " + explainPlanText, !coveringIndexScanPresent);
            rs.close();

            /* perform update */
            statement.executeUpdate(sqlText);

            /* check result */
            expected = "A6 | B6  |    C6     |D6  |\n" +
                    "---------------------------\n" +
                    "11 |NULL |2018-04-22 |1.0 |\n" +
                    "12 |NULL |2018-04-23 |2.0 |\n" +
                    "13 | ccc |2018-04-24 |3.0 |";
            sqlText = "select * from target";
            rs = statement.executeQuery(sqlText);
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();

        } finally {
            conn.rollback();
            conn.setAutoCommit(oldAutoCommit);
        }
    }

    /* putting parenthesis around the left side of the SET hint the parser to put the right side subquery to From clause, and take a different
     * path from that of queries in testUpdateThroughSubquery() */
    @Test
    public void testUpdateThroughFromSubquery() throws Exception {
        setProjectionPruningProperty(projectionPruningDisabled);

        TestConnection conn = methodWatcher.getOrCreateConnection();
        boolean oldAutoCommit = conn.getAutoCommit();
        conn.setAutoCommit(false);
        try (Statement statement = conn.createStatement()) {
            /* case 1, covering index */
            String sqlText = "update target set (b6)=(select 'WWW' from (select * from t5 --splice-properties index=idx_t5\n) dt where a6=a5+10 and a5=1)";
            /* check plan */
            ResultSet rs = statement.executeQuery("explain " + sqlText);

            String explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            boolean coveringIndexScanPresent = explainPlanText.contains("IndexScan") && !explainPlanText.contains("IndexLookup");
            if (projectionPruningDisabled.equals("false"))
                Assert.assertTrue("Query is expected to pick covering index, the current plan is: " + explainPlanText, coveringIndexScanPresent);
            else
                Assert.assertTrue("Query is not expected to pick covering index, the current plan is: " + explainPlanText, !coveringIndexScanPresent);

            rs.close();

            /* perform update */
            statement.executeUpdate(sqlText);

            /* check result */
            String expected = "A6 |B6  |    C6     |D6  |\n" +
                    "--------------------------\n" +
                    "11 |WWW |2018-04-22 |1.0 |\n" +
                    "12 |yyy |2018-04-23 |2.0 |\n" +
                    "13 |zzz |2018-04-24 |3.0 |";
            sqlText = "select * from target";
            rs = statement.executeQuery(sqlText);
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();

            /* case 2, non-covering index */
            sqlText = "update target set (b6)=(select 'VVV' from (select * from t5 --splice-properties index=idx_t5\n) dt where a6=a5+10 and a5=2 and c5=6)";

            rs = statement.executeQuery("explain " + sqlText);

            explainPlanText = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
            coveringIndexScanPresent = explainPlanText.contains("IndexScan") && !explainPlanText.contains("IndexLookup");
            Assert.assertTrue("Query is not expected to pick covering index, the current plan is: " + explainPlanText, !coveringIndexScanPresent);
            rs.close();

            /* perform update */
            statement.executeUpdate(sqlText);

            /* check result */
            expected = "A6 |B6  |    C6     |D6  |\n" +
                    "--------------------------\n" +
                    "11 |WWW |2018-04-22 |1.0 |\n" +
                    "12 |VVV |2018-04-23 |2.0 |\n" +
                    "13 |zzz |2018-04-24 |3.0 |";
            sqlText = "select * from target";
            rs = statement.executeQuery(sqlText);
            assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();

        } finally {
            conn.rollback();
            conn.setAutoCommit(oldAutoCommit);
        }
    }

    @Test
    public void testDB7293() throws Exception {
        setProjectionPruningProperty(projectionPruningDisabled);

        String sqlText = "select a4 from (select a1,b1 from t1 union values (4, 'ddd') except select a4,b4 from t4) dt(a1,b1) left join t4 on dt.a1 = a4";

        String expected = "A4  |\n" +
                "------\n" +
                "NULL |";

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

    }
}
