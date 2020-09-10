package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
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
import static org.junit.Assert.assertEquals;

/**
 * Created by yxia on 1/14/20.
 */
public class PredicateTransitiveClosureIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(PredicateTransitiveClosureIT.class);
    public static final String CLASS_NAME = PredicateTransitiveClosureIT.class.getSimpleName().toUpperCase();
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
                        row(2, 20, 2),
                        row(2, 20, 2),
                        row(3, 30, 3)))
                .create();
        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testTCForConstantExpression() throws Exception {
        String sqlText = "select b1, a1, b2, a2, c2 from --splice-properties joinOrder=fixed\n" +
                "t1, t2 --splice-properties joinStrategy=broadcast\n" +
                "where a1=a2 and a1=1+2";
        String expected = "B1 |A1 |B2 |A2 |C2 |\n" +
                "--------------------\n" +
                "30 | 3 |30 | 3 | 3 |";

        ResultSet rs = methodWatcher.executeQuery("explain " + sqlText);
        String explainString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(rs);
        Assert.assertTrue(sqlText + " expected to derive a2=1+2", explainString.contains("preds=[(A2[2:1] = (1 + 2))])"));
        rs.close();
        rs = methodWatcher.executeQuery(sqlText);
        Assert.assertEquals("\n"+sqlText+"\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testTCForPredicatePushedDownInDT() throws Exception {
        TestConnection conn = methodWatcher.createConnection();
        String sqlText = "select a1 from --splice-properties joinOrder=fixed\n" +
                "(select a1 from (select * from --splice-properties joinOrder=fixed\n" +
                "t1, t3 where a1=a3) dt1 \n" +
                "  inner join \n" +
                "  (select a2 from --splice-properties joinOrder=fixed\n" +
                "t2, t3 where a2=a3) dt2 \n" +
                "  on a1=a2\n" +
                ") dt3 where a1=3";

        /* expected plan
        Plan
        ----
        Cursor(n=21,rows=15,updateMode=READ_ONLY (1),engine=OLTP (default))
          ->  ScrollInsensitive(n=21,totalCost=70.872,outputRows=15,outputHeapSize=65 B,partitions=1)
            ->  ProjectRestrict(n=20,totalCost=57.329,outputRows=15,outputHeapSize=65 B,partitions=1)
              ->  ProjectRestrict(n=18,totalCost=41.011,outputRows=15,outputHeapSize=65 B,partitions=1)
                ->  BroadcastJoin(n=16,totalCost=41.011,outputRows=15,outputHeapSize=65 B,partitions=1,preds=[(A1[16:1] = A2[16:2])])
                  ->  ProjectRestrict(n=14,totalCost=12.278,outputRows=18,outputHeapSize=36 B,partitions=1)
                    ->  BroadcastJoin(n=12,totalCost=12.278,outputRows=18,outputHeapSize=36 B,partitions=1,preds=[(A2[12:1] = A3[12:2])])
                      ->  TableScan[T3(2048)](n=10,totalCost=4.04,scannedRows=20,outputRows=18,outputHeapSize=36 B,partitions=1,preds=[(A3[10:1] = 3)])
                      ->  TableScan[T2(2032)](n=8,totalCost=4.04,scannedRows=20,outputRows=18,outputHeapSize=18 B,partitions=1,preds=[(A2[8:1] = 3)])
                  ->  ProjectRestrict(n=6,totalCost=12.278,outputRows=18,outputHeapSize=36 B,partitions=1)
                    ->  BroadcastJoin(n=4,totalCost=12.278,outputRows=18,outputHeapSize=36 B,partitions=1,preds=[(A1[4:1] = A3[4:2])])
                      ->  TableScan[T3(2048)](n=2,totalCost=4.04,scannedRows=20,outputRows=18,outputHeapSize=36 B,partitions=1,preds=[(A3[2:1] = 3)])
                      ->  TableScan[T1(2016)](n=0,totalCost=4.04,scannedRows=20,outputRows=18,outputHeapSize=18 B,partitions=1,preds=[(A1[0:1] = 3)])

        13 rows selected

         */
        rowContainsQuery(new int[]{7, 8, 11, 12}, "explain " + sqlText, conn,
                new String[]{"TableScan[T3", "preds=[(A3[10:1] = 3)]"},
                new String[]{"TableScan[T2", "preds=[(A2[8:1] = 3)]"},
                new String[]{"TableScan[T3", "preds=[(A3[2:1] = 3)]"},
                new String[]{"TableScan[T1", "preds=[(A1[0:1] = 3)]"});


        String expected = "A1 |\n" +
                "----\n" +
                " 3 |";

        ResultSet rs = conn.query(sqlText);
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        conn.close();
    }
}
