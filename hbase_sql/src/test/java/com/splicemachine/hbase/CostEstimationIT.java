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

package com.splicemachine.hbase;

import com.splicemachine.access.HConfiguration;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by yxia on 5/16/17.
 */
public class CostEstimationIT extends SpliceUnitTest {
    public static final String CLASS_NAME = CostEstimationIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1 (a1 int, b1 int, c1 int)")
                .withInsert("insert into t1 values(?,?,?)")
                .withRows(rows(
                        row(1,1,1),
                        row(2,2,2),
                        row(3,3,3),
                        row(4,4,4),
                        row(5,5,5),
                        row(6,6,6),
                        row(7,7,7),
                        row(8,8,8),
                        row(9,9,9),
                        row(10,10,10)))
                .create();
        for (int i = 0; i < 2; i++) {
            spliceClassWatcher.executeUpdate("insert into t1 select * from t1");
        }

        new TableCreator(conn)
                .withCreate("create table t2 (a2 int, b2 int, c2 int, constraint con1 primary key (a2))")
                .withInsert("insert into t2 values(?,?,?)")
                .withRows(rows(
                        row(1,1,1),
                        row(2,1,1),
                        row(3,1,1),
                        row(4,1,1),
                        row(5,1,1),
                        row(6,2,2),
                        row(7,2,2),
                        row(8,2,2),
                        row(9,2,2),
                        row(10,2,2)))
                .create();

        int factor = 10;
        for (int i = 1; i <= 2; i++) {
            spliceClassWatcher.executeUpdate(format("insert into t2 select a2+%d, b2,c2 from t2", factor));
            factor = factor * 2;
        }

        /* split the table at the value 20 */
        spliceClassWatcher.executeUpdate(format("CALL SYSCS_UTIL.SYSCS_SPLIT_TABLE_OR_INDEX_AT_POINTS('%s', '%s', null, '%s')",
                spliceSchemaWatcher.toString(), "T2", "\\x94"));


        new TableCreator(conn)
                .withCreate("create table t3 (a3 int, b3 int, c3 int, d3 int, primary key (a3, c3))")
                .withInsert("insert into t3 values(?,?,?,?)")
                .withRows(rows(
                        row(1,1,1,1),
                        row(1,2,2,2),
                        row(1,3,3,3),
                        row(1,4,4,4),
                        row(1,5,5,5),
                        row(1,6,6,6),
                        row(1,7,7,7),
                        row(1,8,8,8),
                        row(1,9,9,9),
                        row(1,10,10,10)))
                .create();

        factor = 10;
        for (int i = 1; i <= 8; i++) {
            spliceClassWatcher.executeUpdate(format("insert into t3 select a3, b3+%1$d,c3+%1$d, d3 from t3", factor));
            factor = factor * 2;
        }

        new TableCreator(conn)
                .withCreate("create table t4 (a4 int, b4 int, c4 int, d4 int, primary key (a4, b4))")
                .create();

        spliceClassWatcher.executeUpdate("insert into t4 select * from t3");

        new TableCreator(conn)
                .withCreate("create table t111 (a1 int, b1 int, c1 int)")
                .withInsert("insert into t111 values(?,?,?)")
                .withRows(rows(
                        row(1,1,1),
                        row(2,2,2),
                        row(3,3,3),
                        row(4,4,4),
                        row(5,5,5),
                        row(6,6,6),
                        row(7,7,7),
                        row(8,8,8),
                        row(9,9,9),
                        row(10,10,10)))
                .withIndex("create index idx1_t111 on t111(c1,a1)")
                .withIndex("create index idx2_t111 on t111(b1,a1)")
                .create();

        int multiple = 10;
        for (int i = 0; i < 12; i++) {
            spliceClassWatcher.executeUpdate(format("insert into t111 select a1, b1+%d, c1 from t111", multiple));
            multiple = multiple*2;
        }

        try(PreparedStatement ps = spliceClassWatcher.getOrCreateConnection().
                prepareStatement("analyze schema " + CLASS_NAME)) {
            ps.execute();
        }

        // create more tables that use dummy stats
        new TableCreator(conn)
                .withCreate("create table t11 (a1 int, b1 int, c1 int)")
                .create();
        new TableCreator(conn)
                .withCreate("create table t22 (a2 int, b2 int, c2 int)")
                .create();
        new TableCreator(conn)
                .withCreate("create table t33 (a3 int, b3 int, c3 int)")
                .create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testCardinalityAfterTableSplit() throws Exception {
        SConfiguration config = HConfiguration.getConfiguration();
        HBaseTestingUtility testingUtility = new HBaseTestingUtility((Configuration) config.getConfigSource().unwrapDelegate());
        HBaseAdmin admin = testingUtility.getHBaseAdmin();
        TableName tableName = TableName.valueOf(config.getNamespace(),
                Long.toString(TestUtils.baseTableConglomerateId(spliceClassWatcher.getOrCreateConnection(),
                        spliceSchemaWatcher.toString(), "T2")));

        List<HRegionInfo> regions = admin.getTableRegions(tableName);
        int size1 = regions.size();

        if (size1 >= 2) {
            // expect number of partitions to be at least 2 if table split happens
            String sqlText = "explain select * from --splice-properties joinOrder=fixed \n" +
                    "t1, t2 --splice-properties joinStrategy=NESTEDLOOP \n" +
                    "where c1=c2";

            double outputRows = parseOutputRows(getExplainMessage(4, sqlText, methodWatcher));
            Assert.assertTrue(format("OutputRows is expected to be greater than 1, actual is %s", outputRows), outputRows > 1);

        /* split the table at value 30 */
            methodWatcher.executeUpdate(format("CALL SYSCS_UTIL.SYSCS_SPLIT_TABLE_OR_INDEX_AT_POINTS('%s', '%s', null, '%s')",
                    spliceSchemaWatcher.toString(), "T2", "\\x9E"));

            regions = admin.getTableRegions(tableName);
            int size2 = regions.size();

            if (size2 >= 3) {
                // expect number of partitions to be at least 3 if table split happens
                /**The two newly split partitions do not have stats. Ideally, we should re-collect stats,
                 * but if we haven't, explain should reflect the stats from the remaining partitions.
                 * For current test case, t2 has some partition stats missing, without the fix of SPLICE-1452,
                 * its cardinality estimation assumes unique for all non-null rows, which is too conservative,
                 * so we end up estimating 1 output row from t2 for each outer table row from t1.
                 * With SPLICE-1452's fix, we should see a higher number for the output row from t2.
                 */
                outputRows = parseOutputRows(getExplainMessage(4, sqlText, methodWatcher));
                Assert.assertTrue(format("OutputRows is expected to be greater than 1, actual is %s", outputRows), outputRows > 1);
            }
        }
    }

    @Test
    public void testOuterJoinRowCount() throws Exception {
        /*  t11 is hinted to have a total rowcount of 300, t22 and t33 have the default rowcount of 20.
            The plan is similar to the following:
            --------------------------------------------------------------------
            Cursor(n=10,rows=1,updateMode=READ_ONLY (1),engine=control)
              ->  ScrollInsensitive(n=9,totalCost=141.458,outputRows=1,outputHeapSize=0 B,partitions=1)
                ->  ProjectRestrict(n=8,totalCost=39.315,outputRows=1,outputHeapSize=0 B,partitions=1)
                  ->  GroupBy(n=7,totalCost=39.315,outputRows=1,outputHeapSize=0 B,partitions=1)
                    ->  ProjectRestrict(n=6,totalCost=21.497,outputRows=219,outputHeapSize=332 B,partitions=1)
                      ->  BroadcastLeftOuterJoin(n=5,totalCost=21.497,outputRows=219,outputHeapSize=332 B,partitions=1,preds=[(A2[8:2] = A3[8:3])])
                        ->  TableScan[T33(48224)](n=4,totalCost=4.04,scannedRows=20,outputRows=20,outputHeapSize=332 B,partitions=1)
                        ->  BroadcastJoin(n=3,totalCost=13.039,outputRows=219,outputHeapSize=272 B,partitions=1,preds=[(A1[4:1] = A2[4:2])])
                          ->  TableScan[T22(48208)](n=2,totalCost=4.04,scannedRows=20,outputRows=18,outputHeapSize=272 B,partitions=1,preds=[(A2[2:1] = 90)])
                          ->  TableScan[T11(48192)](n=1,totalCost=4.6,scannedRows=300,outputRows=270,outputHeapSize=270 B,partitions=1,preds=[(A1[0:1] = 90)])

            10 rows selected
         */
        rowContainsQuery(new int[]{2,3,4,5,6,7,8,9,10},"explain select count(*) from --splice-properties joinOrder=fixed\n" +
                        "t11  --splice-properties useDefaultRowCount=300\n" +
                        ", t22 left join t33 --splice-properties joinStrategy=broadcast\n" +
                        "on a2=a3 where a1=a2 and a1=90", methodWatcher,
                "outputRows=1", "outputRows=1", "outputRows=1", "outputRows=219", "outputRows=219", "outputRows=20", "outputRows=219", "outputRows=18", "outputRows=270");

    }

    @Test
    public void testMergeJoinWithVeryNonUniqueJoinCondition() throws Exception {
        // though both source tables are sorted on X.a4=Y.a4, a4 is a very non-unique column (in this case, it has only one value).
        // c4 is a very unique column, but it is not part of the PK that merge join can make use of, so merge join is not attractive
        // for this query
        thirdRowContainsQuery("explain select * from t4 as X, t4 as Y where X.a4=Y.a4 and X.c4=Y.c4","BroadcastJoin",methodWatcher);
    }

    @Test
    public void testMergeJoinWithVeryUniqueJoinCondition() throws Exception {
        thirdRowContainsQuery("explain select * from t3 as X, t3 as Y where X.a3=Y.a3 and X.c3=Y.c3","MergeJoin",methodWatcher);
    }

    /* test case for DB-8061*/
    @Test
    public void testProjectRestrictCostInExplain() throws Exception {
        /* expected plan: step (n=3)'s cost should be consistent with that in step n=2
        Plan
        ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        Cursor(n=7,rows=81000000,updateMode=READ_ONLY (1),engine=Spark)
          ->  ScrollInsensitive(n=6,totalCost=325093388.646,outputRows=81000000,outputHeapSize=231.743 MB,partitions=1)
            ->  ProjectRestrict(n=5,totalCost=281012,outputRows=81000000,outputHeapSize=231.743 MB,partitions=1)
              ->  BroadcastJoin(n=4,totalCost=281012,outputRows=81000000,outputHeapSize=231.743 MB,partitions=1,preds=[(B1[4:5] = B2[4:2])])
                ->  ProjectRestrict(n=3,totalCost=4,outputRows=1,outputHeapSize=231.743 MB,partitions=1)
                  ->  IndexScan[IDX2_T111(2817)](n=2,totalCost=4,scannedRows=1,outputRows=1,outputHeapSize=231.743 MB,partitions=1,baseTable=T111(2784),preds=[(B1[2:1] = 3)])
                ->  TableScan[T22(2848)](n=1,totalCost=200004,scannedRows=100000000,outputRows=90000000,outputHeapSize=257.492 MB,partitions=1,preds=[(B2[0:2] = 3)])

        7 rows selected
         */
        String sqlText = "explain select t111.a1, t111.b1, t22.* from --splice-properties joinOrder=fixed\n" +
                "t22 --splice-properties useDefaultRowCount=100000000\n" +
                ", t111 where b1=b2 and b1=3";

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String projectionStep = getExplainMessage(5, sqlText, methodWatcher);
        Assert.assertTrue(String.format("expected step 5 to be a ProjectRestrict step, actual result was '%s'", projectionStep),
                projectionStep.contains("ProjectRestrict"));
        double projectCost = parseTotalCost(projectionStep);
        String scanStep = getExplainMessage(6, sqlText, methodWatcher);
        Assert.assertTrue(String.format("expected step 6 to be an IndexScan step, actual result was '%s'", scanStep),
                scanStep.contains("IndexScan"));
        double scanCost = parseTotalCost(getExplainMessage(6, sqlText, methodWatcher));

        Assert.assertTrue(format("projectCost is expected to be the same as the scan cost for the index, actual plan is %s",
                TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs)), projectCost==scanCost);
    }

    @Test
    public void testReferenceSelectivityForPredicateWithColumnReferenceFromDT() throws Exception {
        /* expected plan: step (n=6) should access a small number of rows due to the predicate b2=b1
        Plan
        ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        Cursor(n=10,rows=16,updateMode=READ_ONLY (1),engine=control)
          ->  ScrollInsensitive(n=9,totalCost=321.826,outputRows=16,outputHeapSize=262 B,partitions=1)
            ->  NestedLoopJoin(n=8,totalCost=310.652,outputRows=16,outputHeapSize=262 B,partitions=1)
              ->  IndexLookup(n=7,totalCost=8.001,outputRows=1,outputHeapSize=262 B,partitions=1)
                ->  IndexScan[IDX2_T111(2817)](n=6,totalCost=4.001,scannedRows=1,outputRows=1,outputHeapSize=262 B,partitions=1,baseTable=T111(2784),preds=[(B2[7:2] = B1[9:2])])
              ->  ProjectRestrict(n=5,totalCost=12.296,outputRows=16,outputHeapSize=68 B,partitions=1)
                ->  BroadcastJoin(n=4,totalCost=12.296,outputRows=16,outputHeapSize=68 B,partitions=1,preds=[(A2[4:1] = A3[4:4])])
                  ->  TableScan[T33(2864)](n=3,totalCost=4.04,scannedRows=20,outputRows=20,outputHeapSize=68 B,partitions=1)
                  ->  ProjectRestrict(n=2,totalCost=4.04,outputRows=18,outputHeapSize=54 B,partitions=1)
                    ->  TableScan[T22(2848)](n=1,totalCost=4.04,scannedRows=20,outputRows=18,outputHeapSize=54 B,partitions=1,preds=[(A2[0:1] = 3)])

        10 rows selected
         */
        String sqlText = "explain select * from --splice-properties joinOrder=fixed\n" +
                "(select * from t22 where exists (select 1 from t33 where a2=a3)) dt,\n" +
                "t111 --splice-properties joinStrategy=nestedloop, index=idx2_t111\n" +
                "where b2=b1 and a2=3";

        ResultSet rs = methodWatcher.executeQuery(sqlText);

        String indexScanStep = getExplainMessage(5, sqlText, methodWatcher);
        Assert.assertTrue(String.format("expected step 5 to be an IndexScan step, actual result was '%s'", indexScanStep),
                indexScanStep.contains("IndexScan"));
        double scannedRow = parseScannedRows(indexScanStep);

        Assert.assertTrue(format("Index scannedRows is expected to be a small number, actual number is %f, and the plan is %s",
                scannedRow, TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs)), scannedRow < 10);
    }

    @Test
    public void testHashableJoinWithJoinColumnOfInnerTableOnIndexColumn() throws Exception {
        /* the plan should look like the following :
        Plan
        ------------------------------------------------------------------------
        Cursor(n=5,rows=40,updateMode=READ_ONLY (1),engine=control)
          ->  ScrollInsensitive(n=4,totalCost=179.46,outputRows=40,outputHeapSize=640 B,partitions=1)
            ->  BroadcastJoin(n=3,totalCost=15.054,outputRows=40,outputHeapSize=640 B,partitions=1,preds=[(B1[4:2] = A3[4:3])])
              ->  TableScan[T3(2464)](n=2,totalCost=6.97,scannedRows=2560,outputRows=1,outputHeapSize=640 B,partitions=1,preds=[(C3[2:3] = 200)])
              ->  TableScan[T1(2432)](n=1,totalCost=4.045,scannedRows=40,outputRows=40,outputHeapSize=640 B,partitions=1)
        5 rows selected
         */
        String sqlText = "explain select a1, b1, a3, b3, c3 from --splice-properties joinOrder=fixed\n" +
                "t1 inner join t3 --splice-properties joinStrategy=broadcast\n" +
                "on b1=a3\n" +
                "where c3=200";

        rowContainsQuery(new int[]{4,5}, sqlText, methodWatcher,
                new String[] {"TableScan[T3", "scannedRows=2560,outputRows=1"},
                new String[] {"TableScan[T1", "scannedRows=40,outputRows=40"});
    }

    @Test
    public void testDB8715ExplainBug() throws Exception {
        /* the plan should look like the following:
        Plan
        ---------------------------------------------------
        Cursor(n=6,rows=1,updateMode=READ_ONLY (1),engine=control)
          ->  ScrollInsensitive(n=5,totalCost=15.83,outputRows=1,outputHeapSize=2 B,partitions=1)
            ->  ProjectRestrict(n=4,totalCost=12.041,outputRows=1,outputHeapSize=2 B,partitions=1)
              ->  BroadcastJoin(n=3,totalCost=12.041,outputRows=1,outputHeapSize=2 B,partitions=1,preds=[(B1[4:2] = B3[4:5])])
                ->  TableScan[T3(2464)](n=2,totalCost=4,scannedRows=1,outputRows=1,outputHeapSize=2 B,partitions=1,preds=[(A3[2:1] = 200)])
                ->  TableScan[T11(2544)](n=1,totalCost=4.04,scannedRows=20,outputRows=20,outputHeapSize=60 B,partitions=1)
        6 rows selected
         */
        String sqlText = "explain select * from t3 inner join t11 on b1=b3 where a3=200";

        rowContainsQuery(new int[]{5,6}, sqlText, methodWatcher,
                new String[] {"TableScan[T3", "scannedRows=1,outputRows=1"},
                new String[] {"TableScan[T11", "scannedRows=20,outputRows=20"});
    }

    @Test
    public void testUnionAllAsRightOfAJoinWithSubselect() throws Exception {
        /* the plan should look like the following:
        Plan
        ----
        Cursor(n=11,rows=59736,updateMode=READ_ONLY (1),engine=Spark (cost))
          ->  ScrollInsensitive(n=10,totalCost=29274.433,outputRows=59736,outputHeapSize=521.382 KB,partitions=1)
            ->  ProjectRestrict(n=9,totalCost=9552.096,outputRows=59736,outputHeapSize=521.382 KB,partitions=1)
              ->  MergeSortJoin(n=8,totalCost=9552.096,outputRows=59736,outputHeapSize=521.382 KB,partitions=1,preds=[(T1.A1[14:1] = B.A2[14:2])])
                ->  Union(n=7,totalCost=182.482,outputRows=73748,outputHeapSize=288.039 KB,partitions=2)
                  ->  TableScan[T33(2000)](n=6,totalCost=4.04,scannedRows=20,outputRows=20,outputHeapSize=20 B,partitions=1)
                  ->  ProjectRestrict(n=5,totalCost=130.205,outputRows=73728,outputHeapSize=288.02 KB,partitions=1)
                    ->  BroadcastJoin(n=4,totalCost=130.205,outputRows=73728,outputHeapSize=288.02 KB,partitions=1,preds=[(A1[6:1] = A2[6:2])])
                      ->  TableScan[T22(1984)](n=3,totalCost=4.04,scannedRows=20,outputRows=20,outputHeapSize=288.02 KB,partitions=1)
                      ->  IndexScan[IDX1_T111(1937)](n=2,totalCost=48.237,scannedRows=40960,outputRows=40960,outputHeapSize=160 KB,partitions=1,baseTable=T111(1920))
                ->  TableScan[T1(1856)](n=1,totalCost=4.045,scannedRows=40,outputRows=40,outputHeapSize=160 B,partitions=1)

        11 rows selected
         */
        String sqlText = "explain\n" +
                "select 1 from --splice-properties joinOrder=fixed\n" +
                "t1,\n" +
                "(select a2 from t22, t111 where a1=a2\n" +
                "union all\n" +
                "select a3 from t33) b\n" +
                "where t1.a1=b.a2";

        rowContainsQuery(new int[]{4,5,6,7,8,9,10,11}, sqlText, methodWatcher,
                new String[] {"MergeSortJoin", "outputRows=59736"},
                new String[] {"Union", "outputRows=73748"},
                new String[] {"TableScan[T33", "outputRows=20"},
                new String[] {"ProjectRestrict", "outputRows=73728"},
                new String[] {"BroadcastJoin", "outputRows=73728"},
                new String[] {"TableScan[T22", "outputRows=20"},
                new String[] {"IndexScan[IDX1_T111", "outputRows=40960"},
                new String[] {"TableScan[T1", "outputRows=40"}
                );
    }

    @Test
    public void testIntersectAsRightOfAJoinWithSubselect() throws Exception {
        /* the plan should look like the following:
        Plan
        ----
        Cursor(n=11,rows=32,updateMode=READ_ONLY (1),engine=Spark (cost))
          ->  ScrollInsensitive(n=10,totalCost=23598.674,outputRows=32,outputHeapSize=74.393 KB,partitions=1)
            ->  ProjectRestrict(n=9,totalCost=18113.006,outputRows=32,outputHeapSize=74.393 KB,partitions=1)
              ->  BroadcastJoin(n=8,totalCost=18113.006,outputRows=32,outputHeapSize=74.393 KB,partitions=1,preds=[(T1.A1[14:1] = B.SQLCol2[14:2])])
                ->  Intersect(n=7,totalCost=16416.922,outputRows=10,outputHeapSize=74.267 KB,partitions=1)
                  ->  TableScan[T33(1728)](n=6,totalCost=4.04,scannedRows=20,outputRows=20,outputHeapSize=20 B,partitions=1)
                  ->  ProjectRestrict(n=5,totalCost=130.205,outputRows=73728,outputHeapSize=288.02 KB,partitions=1)
                    ->  BroadcastJoin(n=4,totalCost=130.205,outputRows=73728,outputHeapSize=288.02 KB,partitions=1,preds=[(A1[6:1] = A2[6:2])])
                      ->  TableScan[T22(1712)](n=3,totalCost=4.04,scannedRows=20,outputRows=20,outputHeapSize=288.02 KB,partitions=1)
                      ->  IndexScan[IDX1_T111(1665)](n=2,totalCost=48.237,scannedRows=40960,outputRows=40960,outputHeapSize=160 KB,partitions=1,baseTable=T111(1648))
                ->  TableScan[T1(1584)](n=1,totalCost=4.045,scannedRows=40,outputRows=40,outputHeapSize=160 B,partitions=1)

        11 rows selected
         */
        String sqlText = "explain\n" +
                "select 1 from --splice-properties joinOrder=fixed\n" +
                "t1,\n" +
                "(select a2 from t22, t111 where a1=a2\n" +
                "intersect \n" +
                "select a3 from t33) b(a2)\n" +
                "where t1.a1=b.a2";

        rowContainsQuery(new int[]{4,5,6,7,8,9,10,11}, sqlText, methodWatcher,
                new String[] {"BroadcastJoin", "outputRows=32"},
                new String[] {"Intersect", "outputRows=10"},
                new String[] {"TableScan[T33", "outputRows=20"},
                new String[] {"ProjectRestrict", "outputRows=73728"},
                new String[] {"BroadcastJoin", "outputRows=73728"},
                new String[] {"TableScan[T22", "outputRows=20"},
                new String[] {"IndexScan[IDX1_T111", "outputRows=40960"},
                new String[] {"TableScan[T1", "outputRows=40"}
        );
    }
}
