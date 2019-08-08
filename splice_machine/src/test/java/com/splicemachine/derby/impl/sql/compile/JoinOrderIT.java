package com.splicemachine.derby.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.ResultSet;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 * Created by yxia on 6/17/19.
 */
public class JoinOrderIT extends SpliceUnitTest {
    public static final String CLASS_NAME = JoinOrderIT.class.getSimpleName().toUpperCase();
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
                        row(1, 1, 1),
                                row(2, 2, 2),
                                row(3, 3, 3),
                                row(4, 4, 4),
                                row(5, 5, 5),
                                row(6, 6, 6),
                                row(7, 7, 7),
                                row(8, 8, 8),
                                row(9, 9, 9),
                                row(10, 10, 10)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t2 (a2 int, b2 int, c2 int)")
                .withInsert("insert into t2 values(?,?,?)")
                .withRows(rows(
                        row(1, 1, 1),
                                row(2, 2, 2),
                                row(3, 3, 3),
                                row(4, 4, 4),
                                row(5, 5, 5),
                                row(6, 6, 6),
                                row(7, 7, 7),
                                row(8, 8, 8),
                                row(9, 9, 9),
                                row(10, 10, 10)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t3 (a3 int, b3 int, c3 int)")
                .withInsert("insert into t3 values(?,?,?)")
                .withRows(rows(
                        row(1, 1, 1),
                                row(2, 2, 2),
                                row(3, 3, 3),
                                row(4, 4, 4),
                                row(5, 5, 5),
                                row(6, 6, 6),
                                row(7, 7, 7),
                                row(8, 8, 8),
                                row(9, 9, 9),
                                row(10, 10, 10)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t4 (a4 int, b4 int, c4 int)")
                .create();

        new TableCreator(conn)
                .withCreate("create table t5 (a5 int, b5 int, c5 int)")
                .create();

        new TableCreator(conn)
                .withCreate("create table t6 (a6 int, b6 int, c6 int)")
                .create();

        new TableCreator(conn)
                .withCreate("create table t7 (a7 int, b7 int, c7 int)")
                .create();

        spliceClassWatcher.executeQuery(format("analyze schema %s", CLASS_NAME));
        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testEachUnionBranchHasLessThanSixTablesButTotalIsGreater() throws Exception {
        String sqlText = "select a5 from t5, t6, t7\n" +
                        "union all\n" +
                        "select a1 from t1, t2, t3, t4\n" +
                        "where a1=a2 and a1=a3 and a1=a4 and a3=a4";

        ResultSet rs = methodWatcher.executeQuery("explain " + sqlText);

        // we want to check the plan for the second branch, where t4 with 0 rows in a good plan would be joined first
        /* expected plan is similar to the following:
        -------------------------------------------------------------------------------------------------
        Cursor(n=16,rows=2,updateMode=READ_ONLY (1),engine=control)
          ->  ScrollInsensitive(n=15,totalCost=77.564,outputRows=2,outputHeapSize=120 B,partitions=1)
            ->  Union(n=14,totalCost=72.334,outputRows=2,outputHeapSize=120 B,partitions=1)
              ->  ProjectRestrict(n=13,totalCost=28.334,outputRows=1,outputHeapSize=120 B,partitions=1)
                ->  BroadcastJoin(n=12,totalCost=28.334,outputRows=1,outputHeapSize=120 B,partitions=1,preds=[(A1[23:3] = A2[23:4])])
                  ->  TableScan[T2(1712)](n=11,totalCost=4.011,scannedRows=10,outputRows=10,outputHeapSize=120 B,partitions=1)
                  ->  BroadcastJoin(n=10,totalCost=20.222,outputRows=1,outputHeapSize=80 B,partitions=1,preds=[(A1[19:3] = A4[19:1])])
                    ->  TableScan[T1(1696)](n=9,totalCost=4.011,scannedRows=10,outputRows=10,outputHeapSize=80 B,partitions=1)
                    ->  BroadcastJoin(n=8,totalCost=12.111,outputRows=1,outputHeapSize=40 B,partitions=1,preds=[(A3[15:2] = A4[15:1])])
                      ->  TableScan[T3(1728)](n=7,totalCost=4.011,scannedRows=10,outputRows=10,outputHeapSize=40 B,partitions=1)
                      ->  TableScan[T4(1744)](n=6,totalCost=4,scannedRows=1,outputRows=1,outputHeapSize=0 B,partitions=1)
              ->  NestedLoopJoin(n=5,totalCost=32,outputRows=1,outputHeapSize=0 B,partitions=1)
                ->  TableScan[T7(1792)](n=4,totalCost=4,scannedRows=1,outputRows=1,outputHeapSize=0 B,partitions=1)
                ->  NestedLoopJoin(n=3,totalCost=16,outputRows=1,outputHeapSize=0 B,partitions=1)
                  ->  TableScan[T6(1776)](n=2,totalCost=4,scannedRows=1,outputRows=1,outputHeapSize=0 B,partitions=1)
                  ->  TableScan[T5(1760)](n=1,totalCost=4,scannedRows=1,outputRows=1,outputHeapSize=0 B,partitions=1)

        16 rows selected
         */
        int i =0;
        boolean found = false;
        while (rs.next()) {
            String planText = rs.getString(1);
            if (planText!= null && planText.contains("Join")) {
                i++;
                continue;
            }
            // the third join is the very first join of the second branch
            if (i == 3) {
                if (planText != null && planText.contains("TableScan") && planText.contains("T4")) {
                    found = true;
                    break;
                }
            }
        }

        rs.close();

        Assert.assertTrue("T4 is expected to appear in the first join", found);
    }

    @Test
    public void testEachUnionBranchHasLessThanSixTablesAndTotalIsAlsoLess() throws Exception {
        String sqlText = "select a5 from t5\n" +
                "union all\n" +
                "select a1 from t1, t2, t3, t4\n" +
                "where a1=a2 and a1=a3 and a1=a4 and a3=a4";

        ResultSet rs = methodWatcher.executeQuery("explain " + sqlText);

        // we want to check the plan for the second branch, where t4 with 0 rows in a good plan would be joined first
        /* expected plan is similar to the following:
        -------------------------------------------------------------------------------------------------
        Cursor(n=12,rows=2,updateMode=READ_ONLY (1),engine=control)
          ->  ScrollInsensitive(n=11,totalCost=41.564,outputRows=2,outputHeapSize=120 B,partitions=1)
            ->  Union(n=10,totalCost=36.334,outputRows=2,outputHeapSize=120 B,partitions=1)
              ->  ProjectRestrict(n=9,totalCost=28.334,outputRows=1,outputHeapSize=120 B,partitions=1)
                ->  BroadcastJoin(n=8,totalCost=28.334,outputRows=1,outputHeapSize=120 B,partitions=1,preds=[(A1[15:3] = A2[15:4])])
                  ->  TableScan[T2(1600)](n=7,totalCost=4.011,scannedRows=10,outputRows=10,outputHeapSize=120 B,partitions=1)
                  ->  BroadcastJoin(n=6,totalCost=20.222,outputRows=1,outputHeapSize=80 B,partitions=1,preds=[(A1[11:3] = A4[11:1])])
                    ->  TableScan[T1(1584)](n=5,totalCost=4.011,scannedRows=10,outputRows=10,outputHeapSize=80 B,partitions=1)
                    ->  BroadcastJoin(n=4,totalCost=12.111,outputRows=1,outputHeapSize=40 B,partitions=1,preds=[(A3[7:2] = A4[7:1])])
                      ->  TableScan[T3(1616)](n=3,totalCost=4.011,scannedRows=10,outputRows=10,outputHeapSize=40 B,partitions=1)
                      ->  TableScan[T4(1632)](n=2,totalCost=4,scannedRows=1,outputRows=1,outputHeapSize=0 B,partitions=1)
              ->  TableScan[T5(1648)](n=1,totalCost=4,scannedRows=1,outputRows=1,outputHeapSize=0 B,partitions=1)
        12 rows selected
         */
        int i =0;
        boolean found = false;
        while (rs.next()) {
            String planText = rs.getString(1);
            if (planText!= null && planText.contains("Join")) {
                i++;
                continue;
            }
            // the third join is the very first join of the second branch
            if (i == 3) {
                if (planText != null && planText.contains("TableScan") && planText.contains("T4")) {
                    found = true;
                    break;
                }
            }
        }

        rs.close();

        Assert.assertTrue("T4 is expected to appear in the first join", found);
    }
}
