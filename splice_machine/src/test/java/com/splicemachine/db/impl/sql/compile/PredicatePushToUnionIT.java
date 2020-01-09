package com.splicemachine.db.impl.sql.compile;

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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Created by yxia on 5/30/19.
 */
@RunWith(Parameterized.class)
public class PredicatePushToUnionIT extends SpliceUnitTest {

    private Boolean useSpark;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }

    public static final String CLASS_NAME = PredicatePushToUnionIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public PredicatePushToUnionIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1 (a1 char(10), b1 int, c1 int, primary key(a1, b1))")
                .withInsert("insert into t1 values (?,?,?)")
                .withRows(rows(
                        row("A", 1, 1),
                        row("B", 2, 2),
                        row("C", 3, 3),
                        row("D", 4, 4),
                        row("E", 5, 0),
                        row("F", 6, 1),
                        row("G", 7, 2),
                        row("H", 8, 3),
                        row("I", 9, 4),
                        row("J", 10, 0)))
                .withIndex("create index idx_t1 on t1(c1, b1)")
                .create();
        int increment = 10;
        for (int i = 0; i < 3; i++) {
            spliceClassWatcher.executeUpdate(format("insert into t1 select a1, b1+%d, c1 from t1", increment));
            increment *= 2;
        }

        new TableCreator(conn)
                .withCreate("create table t2 (a2 char(10), b2 int, c2 int, primary key(a2, b2))")
                .withIndex("create index idx_t2 on t2(c2, b2)")
                .create();

        spliceClassWatcher.executeUpdate(format("insert into t2 select a1, b1, c1 from t1 where b1 between 1 and 10"));

        new TableCreator(conn)
                .withCreate("create table t3 (a3 char(10), b3 int, c3 int, primary key(a3, b3))")
                .create();

        spliceClassWatcher.executeUpdate(format("insert into t3 select a1, b1, c1 from t1 where b1 between 21 and 30"));

        new TableCreator(conn)
                .withCreate("create table t11 (a1 char(10), b1 int, c1 int, primary key(a1, b1))")
                .create();

        spliceClassWatcher.executeUpdate(format("insert into t11 select a1, b1, c1 from t1 where b1 between 1 and 10"));

        // create view
        spliceClassWatcher.execute("create view v1(Y, Z) as (select b2 as Y, c2 as Z from t2 \n" +
                "                  union all \n" +
                "                  select b3 as Y, c3 as Z from t3)");

        new TableCreator(conn)
                .withCreate("create table t5 (a5 varchar(10), b5 int, c5 date, primary key(a5, b5,c5))")
                .withIndex("create index idx_t5 on t5(c5)")
                .withInsert("insert into t5 values (?,?,?)")
                .withRows(rows(
                        row("abcde", 1, "2018-12-01"),
                        row("abcde", 1, "2018-12-02"),
                        row("abcde", 1, "2018-12-03"),
                        row("abcde", 1, "2018-12-04"),
                        row("abcde", 1, "2018-12-05"),
                        row("abcde", 2, "2018-12-01"),
                        row("abcde", 2, "2018-12-02"),
                        row("abcde", 2, "2018-12-03"),
                        row("abcde", 2, "2018-12-04"),
                        row("abcde", 2, "2018-12-05"),
                        row("hijkl", 1, "2018-12-01"),
                        row("hijkl", 1, "2018-12-02"),
                        row("hijkl", 1, "2018-12-03"),
                        row("hijkl", 1, "2018-12-04"),
                        row("hijkl", 1, "2018-12-05"),
                        row("hijkl", 2, "2018-12-01"),
                        row("hijkl", 2, "2018-12-02"),
                        row("hijkl", 2, "2018-12-03"),
                        row("hijkl", 2, "2018-12-04"),
                        row("hijkl", 2, "2018-12-05")))
                .create();

        new TableCreator(conn)
                .withCreate("create table t6 (a6 varchar(10), b6 int, c6 date, primary key(a6, b6, c6))")
                .withIndex("create index idx_t6 on t6(c6)")
                .withInsert("insert into t6 values (?,?,?)")
                .withRows(rows(
                        row("opqrs", 1, "2018-12-01"),
                        row("opqrs", 1, "2018-12-02"),
                        row("opqrs", 1, "2018-12-03"),
                        row("opqrs", 1, "2018-12-04"),
                        row("opqrs", 1, "2018-12-05"),
                        row("opqrs", 2, "2018-12-01"),
                        row("opqrs", 2, "2018-12-02"),
                        row("opqrs", 2, "2018-12-03"),
                        row("opqrs", 2, "2018-12-04"),
                        row("opqrs", 2, "2018-12-05"),
                        row("splice", 1, "2018-12-01"),
                        row("splice", 1, "2018-12-02"),
                        row("splice", 1, "2018-12-03"),
                        row("splice", 1, "2018-12-04"),
                        row("splice", 1, "2018-12-05"),
                        row("splice", 2, "2018-12-01"),
                        row("splice", 2, "2018-12-02"),
                        row("splice", 2, "2018-12-03"),
                        row("splice", 2, "2018-12-04"),
                        row("splice", 2, "2018-12-05")))
                .create();

        new TableCreator(conn)
                .withCreate("create table t7 (a7 char(5), b7 char(5), c7 int, primary key(a7, b7))")
                .withInsert("insert into t7 values (?,?,?)")
                .withRows(rows(
                        row("AAAAA", "AAAAA", 1),
                        row("BBB", "BBB", 2),
                        row("CCCC", "CCCC", 3)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t8 (a8 char(3), b8 char(3), c8 int, primary key(a8, b8))")
                .withInsert("insert into t8 values (?,?,?)")
                .withRows(rows(
                        row("aaa", "aaa", 1),
                        row("b", "b", 2),
                        row("cc", "cc", 3)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t9 (a9 varchar(5), b9 varchar(5), c9 int, primary key(a9, b9))")
                .withInsert("insert into t9 values (?,?,?)")
                .withRows(rows(
                        row("AAAAA", "AAAAA", 1),
                        row("BBB", "BBB", 2),
                        row("BBB  ", "BBB  ", 22),
                        row("CCCC", "CCCC", 3)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t10 (a10 varchar(3), b10 varchar(3), c10 int, primary key(a10, b10))")
                .withInsert("insert into t10 values (?,?,?)")
                .withRows(rows(
                        row("aaa", "aaa", 1),
                        row("b", "b", 2),
                        row("b  ", "b  ", 22),
                        row("cc", "cc", 3)))
                .create();

    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testPushPredicateIntoUnionAll() throws Exception {
        String sqlText = format("select Y, Z from (" +
                "select b1 as Y, c1 as Z from t1 --splice-properties useSpark=%s\n" +
                "  union all \n" +
                "  select b2 as Y, c2 as Z from t2 \n" +
                "  union all \n" +
                "  select b3 as Y, c3 as Z from t3) dt where Z=1 and Y=1", useSpark);

        /* plan should look like the following:
        Plan
 ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        Cursor(n=10,rows=51,updateMode=READ_ONLY (1),engine=control)
          ->  ScrollInsensitive(n=9,totalCost=24.61,outputRows=51,outputHeapSize=34 B,partitions=1)
            ->  ProjectRestrict(n=8,totalCost=20.439,outputRows=51,outputHeapSize=34 B,partitions=1,preds=[(Y[10:1] = 1),(Z[10:2] = 1)])
              ->  Union(n=7,totalCost=20.439,outputRows=51,outputHeapSize=34 B,partitions=1)
                ->  TableScan[T3(1696)](n=6,totalCost=4.04,scannedRows=20,outputRows=17,outputHeapSize=34 B,partitions=1,preds=[(C3[7:2] = 1),(B3[7:1] = 1)])
                ->  Union(n=5,totalCost=12.228,outputRows=34,outputHeapSize=34 B,partitions=1)
                  ->  ProjectRestrict(n=4,totalCost=4.028,outputRows=17,outputHeapSize=34 B,partitions=1)
                    ->  IndexScan[IDX_T2(1681)](n=3,totalCost=4.028,scannedRows=17,outputRows=17,outputHeapSize=34 B,partitions=1,baseTable=T2(1664),preds=[(C2[3:1] = 1),(B2[3:2] = 1)])
                  ->  ProjectRestrict(n=2,totalCost=4.028,outputRows=17,outputHeapSize=34 B,partitions=1)
                    ->  IndexScan[IDX_T1(1649)](n=1,totalCost=4.028,scannedRows=17,outputRows=17,outputHeapSize=34 B,partitions=1,baseTable=T1(1632),preds=[(C1[0:1] = 1),(B1[0:2] = 1)])

        10 rows selected
         */
        rowContainsQuery(new int[]{5, 8, 10}, "explain " + sqlText, methodWatcher,
                new String[]{"TableScan", "preds=[(C3[7:2] = 1),(B3[7:1] = 1)]"},
                new String[]{"IndexScan", "preds=[(C2[3:1] = 1),(B2[3:2] = 1)]"},
                new String[]{"IndexScan", "preds=[(C1[0:1] = 1),(B1[0:2] = 1)]"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "Y | Z |\n" +
                        "--------\n" +
                        " 1 | 1 |\n" +
                        " 1 | 1 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testUnionAllWithConstantProjected() throws Exception {
        // use a constant value 11 for Y in the first branch, Y=1 won't be pushed to this branch, but it will
        // still be pushed to the other branches
        String sqlText = format("select Y, Z from (" +
                "select 11 as Y, c1 as Z from t1 --splice-properties useSpark=%s\n" +
                "  union all \n" +
                "  select b2 as Y, c2 as Z from t2 \n" +
                "  union all \n" +
                "  select b3 as Y, c3 as Z from t3) dt where Z=1 and Y=1", useSpark);

        /* plan should look like the following:
        Plan
        -------------------------------------------------------------------------------------------------------------------
        Cursor(n=10,rows=52,updateMode=READ_ONLY (1),engine=control)
          ->  ScrollInsensitive(n=9,totalCost=24.62,outputRows=52,outputHeapSize=34 B,partitions=1)
            ->  ProjectRestrict(n=8,totalCost=20.449,outputRows=52,outputHeapSize=34 B,partitions=1,preds=[(Y[10:1] = 1),(Z[10:2] = 1)])
              ->  Union(n=7,totalCost=20.449,outputRows=52,outputHeapSize=34 B,partitions=1)
                ->  TableScan[T3(3568)](n=6,totalCost=4.04,scannedRows=20,outputRows=17,outputHeapSize=34 B,partitions=1,preds=[(C3[7:2] = 1),(B3[7:1] = 1)])
                ->  Union(n=5,totalCost=12.238,outputRows=35,outputHeapSize=34 B,partitions=1)
                  ->  ProjectRestrict(n=4,totalCost=4.028,outputRows=17,outputHeapSize=34 B,partitions=1)
                    ->  IndexScan[IDX_T2(3553)](n=3,totalCost=4.028,scannedRows=17,outputRows=17,outputHeapSize=34 B,partitions=1,baseTable=T2(3536),preds=[(C2[3:1] = 1),(B2[3:2] = 1)])
                  ->  ProjectRestrict(n=2,totalCost=4.03,outputRows=18,outputHeapSize=18 B,partitions=1)
                    ->  IndexScan[IDX_T1(3521)](n=1,totalCost=4.03,scannedRows=18,outputRows=18,outputHeapSize=18 B,partitions=1,baseTable=T1(3504),preds=[(C1[0:1] = 1)])

        10 rows selected
         */
        rowContainsQuery(new int[]{5, 8, 10}, "explain " + sqlText, methodWatcher,
                new String[]{"TableScan", "preds=[(C3[7:2] = 1),(B3[7:1] = 1)]"},
                new String[]{"IndexScan", "preds=[(C2[3:1] = 1),(B2[3:2] = 1)]"},
                new String[]{"IndexScan", "preds=[(C1[0:1] = 1)]"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "Y | Z |\n" +
                        "--------\n" +
                        " 1 | 1 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testPushPredicateIntoUnion() throws Exception {
        // use dt(X,Y)
        String sqlText = format("select X, Y from (" +
                "select a1, b1 from t1 --splice-properties useSpark=%s\n" +
                "  union \n" +
                "  select a2, b2 from t2 \n" +
                "  union \n" +
                "  select a3, b3 from t3) dt(X,Y) where X in ('A', 'B', 'C')", useSpark);

        /* plan should look like the following:
        Plan
        ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
        Cursor(n=10,rows=54,updateMode=READ_ONLY (1),engine=control)
          ->  ScrollInsensitive(n=9,totalCost=28.828,outputRows=54,outputHeapSize=36 B,partitions=1)
            ->  ProjectRestrict(n=8,totalCost=20.468,outputRows=54,outputHeapSize=36 B,partitions=1,preds=[(SQLCol3[14:1] IN (A         ,B         ,C         ))])
              ->  Distinct(n=7,totalCost=20.468,outputRows=54,outputHeapSize=36 B,partitions=1)
                ->  Union(n=6,totalCost=20.468,outputRows=54,outputHeapSize=36 B,partitions=1)
                  ->  MultiProbeTableScan[T3(1776)](n=5,totalCost=4.036,scannedRows=18,outputRows=18,outputHeapSize=36 B,partitions=1,preds=[(A3[9:1] IN (A         ,B         ,C         ))])
                  ->  Distinct(n=4,totalCost=12.252,outputRows=36,outputHeapSize=36 B,partitions=1)
                    ->  Union(n=3,totalCost=12.252,outputRows=36,outputHeapSize=36 B,partitions=1)
                      ->  MultiProbeTableScan[T2(1744)](n=2,totalCost=4.036,scannedRows=18,outputRows=18,outputHeapSize=36 B,partitions=1,preds=[(A2[3:1] IN (A         ,B         ,C         ))])
                      ->  MultiProbeTableScan[T1(1712)](n=1,totalCost=4.036,scannedRows=18,outputRows=18,outputHeapSize=36 B,partitions=1,preds=[(A1[0:1] IN (A         ,B         ,C         ))])

        10 rows selected
         */
        rowContainsQuery(new int[]{6, 9, 10}, "explain " + sqlText, methodWatcher,
                new String[]{"MultiProbeTableScan", "preds=[(A3[9:1] IN (A         ,B         ,C         ))]"},
                new String[]{"MultiProbeTableScan", "preds=[(A2[3:1] IN (A         ,B         ,C         ))]"},
                new String[]{"MultiProbeTableScan", "preds=[(A1[0:1] IN (A         ,B         ,C         ))]"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "X | Y |\n" +
                        "--------\n" +
                        " A | 1 |\n" +
                        " A |11 |\n" +
                        " A |21 |\n" +
                        " A |31 |\n" +
                        " A |41 |\n" +
                        " A |51 |\n" +
                        " A |61 |\n" +
                        " A |71 |\n" +
                        " B |12 |\n" +
                        " B | 2 |\n" +
                        " B |22 |\n" +
                        " B |32 |\n" +
                        " B |42 |\n" +
                        " B |52 |\n" +
                        " B |62 |\n" +
                        " B |72 |\n" +
                        " C |13 |\n" +
                        " C |23 |\n" +
                        " C | 3 |\n" +
                        " C |33 |\n" +
                        " C |43 |\n" +
                        " C |53 |\n" +
                        " C |63 |\n" +
                        " C |73 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

    }

    @Test
    public void testUnionAllView() throws Exception {
        String sqlText = format("select Y, Z from (select * from v1 left join t11 on Y=t11.b1) dt where Z=1 and Y=1", useSpark);

        /* plan should look like the following:
        Plan
        -------------------------------------------------------------------------------------------------------------------
        Cursor(n=10,rows=34,updateMode=READ_ONLY (1),engine=control)
          ->  ScrollInsensitive(n=9,totalCost=23.739,outputRows=34,outputHeapSize=128 B,partitions=2)
            ->  ProjectRestrict(n=8,totalCost=8.257,outputRows=34,outputHeapSize=128 B,partitions=2)
              ->  BroadcastLeftOuterJoin(n=7,totalCost=8.257,outputRows=34,outputHeapSize=128 B,partitions=2,preds=[(Y[10:1] = T11.B1[10:3])])
                ->  TableScan[T11(2944)](n=6,totalCost=4.04,scannedRows=20,outputRows=20,outputHeapSize=128 B,partitions=2)
                ->  ProjectRestrict(n=5,totalCost=12.239,outputRows=34,outputHeapSize=34 B,partitions=1,preds=[(V1.Z[6:2] = 1),(V1.Y[6:1] = 1)])
                  ->  Union(n=4,totalCost=12.239,outputRows=34,outputHeapSize=34 B,partitions=1)
                    ->  TableScan[T3(2928)](n=3,totalCost=4.04,scannedRows=20,outputRows=17,outputHeapSize=34 B,partitions=1,preds=[(C3[3:2] = 1),(B3[3:1] = 1)])
                    ->  ProjectRestrict(n=2,totalCost=4.028,outputRows=17,outputHeapSize=34 B,partitions=1)
                      ->  IndexScan[IDX_T2(2913)](n=1,totalCost=4.028,scannedRows=17,outputRows=17,outputHeapSize=34 B,partitions=1,baseTable=T2(2896),preds=[(C2[0:1] = 1),(B2[0:2] = 1)])
        10 rows selected
         */
        rowContainsQuery(new int[]{8, 10}, "explain " + sqlText, methodWatcher,
                new String[]{"preds=[(C3[3:2] = 1),(B3[3:1] = 1)]"},
                new String[]{"IndexScan", "preds=[(C2[0:1] = 1),(B2[0:2] = 1)]"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "Y | Z |\n" +
                        "--------\n" +
                        " 1 | 1 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testPushDownOfMultipleInLists() throws Exception {
        String sqlText = format("select X, Y, Z from (\n" +
                "                select c5, b5, a5 from t5 --splice-properties useSpark=%1$s \n" +
                "                     where b5=2 \n" +
                "                union all \n" +
                "                select c6, b6, a6 from t6 --splice-properties useSpark=%1$s \n\n" +
                "                     where b6=2) dt(Z, Y, X) where X in ('abcde', 'splice') and Z in ('2018-12-01', '2018-12-05')", useSpark);

        /* Control plan should look like the following:
        Plan
        ----------------------------------------------------------------------------------------------------------------------
        Cursor(n=9,rows=34,updateMode=READ_ONLY (1),engine=control)
          ->  ScrollInsensitive(n=8,totalCost=16.4,outputRows=34,outputHeapSize=49 B,partitions=1)
            ->  ProjectRestrict(n=7,totalCost=12.234,outputRows=34,outputHeapSize=49 B,partitions=1)
              ->  ProjectRestrict(n=6,totalCost=12.234,outputRows=34,outputHeapSize=49 B,partitions=1,preds=[(SQLCol4[6:1] IN (2018-12-01,2018-12-05)),(SQLCol6[6:3] IN (abcde,splice))])
                ->  Union(n=5,totalCost=12.234,outputRows=34,outputHeapSize=49 B,partitions=1)
                  ->  ProjectRestrict(n=4,totalCost=4.034,outputRows=17,outputHeapSize=49 B,partitions=1)
                    ->  MultiProbeTableScan[T6(3888)](n=3,totalCost=4.034,scannedRows=17,outputRows=17,outputHeapSize=49 B,partitions=1,preds=[((A6[3:1],B6[3:2],C6[3:3]) IN ((abcde,2,2018-12-01),(abcde,2,2018-12-05),(splice,2,2018-12-01),(splice,2,2018-12-05)))])
                  ->  ProjectRestrict(n=2,totalCost=4.034,outputRows=17,outputHeapSize=49 B,partitions=1)
                    ->  MultiProbeTableScan[T5(3856)](n=1,totalCost=4.034,scannedRows=17,outputRows=17,outputHeapSize=49 B,partitions=1,preds=[((A5[0:1],B5[0:2],C5[0:3]) IN ((abcde,2,2018-12-01),(abcde,2,2018-12-05),(splice,2,2018-12-01),(splice,2,2018-12-05)))])

        9 rows selected
         */
        /* Spark plan should look like the following:
        Plan
        ----
        Cursor(n=11,rows=34,updateMode=READ_ONLY (1),engine=Spark)
          ->  ScrollInsensitive(n=10,totalCost=16.4,outputRows=34,outputHeapSize=49 B,partitions=1)
            ->  ProjectRestrict(n=9,totalCost=12.234,outputRows=34,outputHeapSize=49 B,partitions=1)
              ->  ProjectRestrict(n=8,totalCost=12.234,outputRows=34,outputHeapSize=49 B,partitions=1,preds=[(SQLCol4[6:1] IN (2018-12-01,2018-12-05)),(SQLCol6[6:3] IN (abcde,splice))])
                ->  Union(n=7,totalCost=12.234,outputRows=34,outputHeapSize=49 B,partitions=1)
                  ->  ProjectRestrict(n=6,totalCost=4.034,outputRows=17,outputHeapSize=49 B,partitions=1)
                    ->  ProjectRestrict(n=5,totalCost=4.034,outputRows=17,outputHeapSize=49 B,partitions=1,preds=[(C6[3:3] IN (2018-12-01,2018-12-05))])
                      ->  MultiProbeTableScan[T6(6288)](n=4,totalCost=4.034,scannedRows=17,outputRows=17,outputHeapSize=49 B,partitions=1,preds=[(A6[3:1] IN (abcde,splice)),(B6[3:2] = 2)])
                  ->  ProjectRestrict(n=3,totalCost=4.034,outputRows=17,outputHeapSize=49 B,partitions=1)
                    ->  ProjectRestrict(n=2,totalCost=4.034,outputRows=17,outputHeapSize=49 B,partitions=1,preds=[(C5[0:3] IN (2018-12-01,2018-12-05))])
                      ->  MultiProbeTableScan[T5(6256)](n=1,totalCost=4.034,scannedRows=17,outputRows=17,outputHeapSize=49 B,partitions=1,preds=[(A5[0:1] IN (abcde,splice)),(B5[0:2] = 2)])

        11 rows selected
         */

        /* for spark path, only one inlist can be used in the access path, so the plans under spark and control are different */
        if (!useSpark) {
            rowContainsQuery(new int[]{7, 9}, "explain " + sqlText, methodWatcher,
                    new String[]{"MultiProbeTableScan", "preds=[((A6[3:1],B6[3:2],C6[3:3]) IN ((abcde,2,2018-12-01),(abcde,2,2018-12-05),(splice,2,2018-12-01),(splice,2,2018-12-05)))]"},
                    new String[]{"MultiProbeTableScan", "preds=[((A5[0:1],B5[0:2],C5[0:3]) IN ((abcde,2,2018-12-01),(abcde,2,2018-12-05),(splice,2,2018-12-01),(splice,2,2018-12-05)))]"});
        } else {
            rowContainsQuery(new int[]{7, 8, 10, 11}, "explain " + sqlText, methodWatcher,
                    new String[]{"ProjectRestrict", "preds=[(C6[3:3] IN (2018-12-01,2018-12-05))]"},
                    new String[]{"MultiProbeTableScan", "preds=[(A6[3:1] IN (abcde,splice)),(B6[3:2] = 2)]"},
                    new String[]{"ProjectRestrict", "preds=[(C5[0:3] IN (2018-12-01,2018-12-05))]"},
                    new String[]{"MultiProbeTableScan", "preds=[(A5[0:1] IN (abcde,splice)),(B5[0:2] = 2)]"});
        }


        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "X   | Y |     Z     |\n" +
                        "------------------------\n" +
                        " abcde | 2 |2018-12-01 |\n" +
                        " abcde | 2 |2018-12-05 |\n" +
                        "splice | 2 |2018-12-01 |\n" +
                        "splice | 2 |2018-12-05 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testPushDownInListWithExpressions() throws Exception {
        String sqlText = format("select X, Y, Z from (\n" +
                "                  select c5, b5, a5 from t5 --splice-properties useSpark=%1$s \n" +
                "                     where b5=2 \n" +
                "                  union all \n" +
                "                  select c6, b6, a6 from t6 --splice-properties useSpark=%1$s \n" +
                "                     where b6=2) dt(Z, Y, X) " +
                "                where X in (substr('abcdeNNN', 1,5), 'splice') and " +
                "                  Z in (add_months('2018-10-01', 2), add_months('2018-11-05',2))", useSpark);

        /* control plan should look like the following:
        Plan
        ------------------------------------------------------------------------------------------------------------------
        Cursor(n=9,rows=2,updateMode=READ_ONLY (1),engine=control)
          ->  ScrollInsensitive(n=8,totalCost=16.092,outputRows=2,outputHeapSize=1 B,partitions=1)
            ->  ProjectRestrict(n=7,totalCost=12.086,outputRows=2,outputHeapSize=1 B,partitions=1)
              ->  ProjectRestrict(n=6,totalCost=12.086,outputRows=2,outputHeapSize=1 B,partitions=1,preds=[(SQLCol4[6:1] IN (dataTypeServices: DATE ,dataTypeServices: DATE )),(SQLCol6[6:3] IN (substring(abcdeNNN, 1, 5) ,splice))])
                ->  Union(n=5,totalCost=12.086,outputRows=2,outputHeapSize=1 B,partitions=1)
                  ->  ProjectRestrict(n=4,totalCost=4.04,outputRows=1,outputHeapSize=1 B,partitions=1)
                    ->  MultiProbeTableScan[T6(4368)](n=3,totalCost=4.04,scannedRows=20,outputRows=20,outputHeapSize=1 B,partitions=1,preds=[((A6[3:1],B6[3:2],C6[3:3]) IN ((substring(abcdeNNN, 1, 5) ,2,dataTypeServices: DATE ),(substring(abcdeNNN, 1, 5) ,2,dataTypeServices: DATE ),(splice,2,dataTypeServices: DATE ),(splice,2,dataTypeServices: DATE )))])
                  ->  ProjectRestrict(n=2,totalCost=4.04,outputRows=1,outputHeapSize=1 B,partitions=1)
                    ->  MultiProbeTableScan[T5(4336)](n=1,totalCost=4.04,scannedRows=20,outputRows=20,outputHeapSize=1 B,partitions=1,preds=[((A5[0:1],B5[0:2],C5[0:3]) IN ((substring(abcdeNNN, 1, 5) ,2,dataTypeServices: DATE ),(substring(abcdeNNN, 1, 5) ,2,dataTypeServices: DATE ),(splice,2,dataTypeServices: DATE ),(splice,2,dataTypeServices: DATE )))])

        9 rows selected
        */
        /* spark plan looks like the following:
        Plan
        ----
        Cursor(n=11,rows=2,updateMode=READ_ONLY (1),engine=Spark)
          ->  ScrollInsensitive(n=10,totalCost=16.02,outputRows=2,outputHeapSize=1 B,partitions=1)
            ->  ProjectRestrict(n=9,totalCost=12.014,outputRows=2,outputHeapSize=1 B,partitions=1)
              ->  ProjectRestrict(n=8,totalCost=12.014,outputRows=2,outputHeapSize=1 B,partitions=1,preds=[(SQLCol4[6:1] IN (dataTypeServices: DATE ,dataTypeServices: DATE )),(SQLCol6[6:3] IN (substring(abcdeNNN, 1, 5) ,splice))])
                ->  Union(n=7,totalCost=12.014,outputRows=2,outputHeapSize=1 B,partitions=1)
                  ->  ProjectRestrict(n=6,totalCost=4.004,outputRows=1,outputHeapSize=1 B,partitions=1)
                    ->  ProjectRestrict(n=5,totalCost=4.004,outputRows=1,outputHeapSize=1 B,partitions=1,preds=[(C6[3:3] IN (dataTypeServices: DATE ,dataTypeServices: DATE ))])
                      ->  MultiProbeTableScan[T6(6288)](n=4,totalCost=4.004,scannedRows=2,outputRows=2,outputHeapSize=1 B,partitions=1,preds=[(A6[3:1] IN (substring(abcdeNNN, 1, 5) ,splice)),(B6[3:2] = 2)])
                  ->  ProjectRestrict(n=3,totalCost=4.004,outputRows=1,outputHeapSize=1 B,partitions=1)
                    ->  ProjectRestrict(n=2,totalCost=4.004,outputRows=1,outputHeapSize=1 B,partitions=1,preds=[(C5[0:3] IN (dataTypeServices: DATE ,dataTypeServices: DATE ))])
                      ->  MultiProbeTableScan[T5(6256)](n=1,totalCost=4.004,scannedRows=2,outputRows=2,outputHeapSize=1 B,partitions=1,preds=[(A5[0:1] IN (substring(abcdeNNN, 1, 5) ,splice)),(B5[0:2] = 2)])

        11 rows selected
         */
        if (!useSpark) {
            rowContainsQuery(new int[]{7, 9}, "explain " + sqlText, methodWatcher,
                    new String[]{"MultiProbeTableScan", "preds=[((A6[3:1],B6[3:2],C6[3:3]) IN ((substring(abcdeNNN, 1, 5) ,2,dataTypeServices: DATE ),(substring(abcdeNNN, 1, 5) ,2,dataTypeServices: DATE ),(splice,2,dataTypeServices: DATE ),(splice,2,dataTypeServices: DATE )))]"},
                    new String[]{"MultiProbeTableScan", "preds=[((A5[0:1],B5[0:2],C5[0:3]) IN ((substring(abcdeNNN, 1, 5) ,2,dataTypeServices: DATE ),(substring(abcdeNNN, 1, 5) ,2,dataTypeServices: DATE ),(splice,2,dataTypeServices: DATE ),(splice,2,dataTypeServices: DATE )))]"});
        } else {
            rowContainsQuery(new int[]{7, 8, 10, 11}, "explain " + sqlText, methodWatcher,
                    new String[]{"ProjectRestrict", "preds=[(C6[3:3] IN (dataTypeServices: DATE ,dataTypeServices: DATE ))]"},
                    new String[]{"MultiProbeTableScan", "preds=[(A6[3:1] IN (substring(abcdeNNN, 1, 5) ,splice)),(B6[3:2] = 2)]"},
                    new String[]{"ProjectRestrict", "preds=[(C5[0:3] IN (dataTypeServices: DATE ,dataTypeServices: DATE ))]"},
                    new String[]{"MultiProbeTableScan", "preds=[(A5[0:1] IN (substring(abcdeNNN, 1, 5) ,splice)),(B5[0:2] = 2)]"});
        }

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "X   | Y |     Z     |\n" +
                        "------------------------\n" +
                        " abcde | 2 |2018-12-01 |\n" +
                        "splice | 2 |2018-12-01 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

    }

    @Test
    public void testUnionAllBranchHasInnerJoin() throws Exception {
        String sqlText = format("select Y, Z, W from (" +
                "select t1.b1, t1.c1, t11.a1 from --splice-properties joinOrder=fixed\n " +
                "t1 --splice-properties useSpark=%s\n, t11 where t1.b1 = t11.b1 " +
                "  union all \n" +
                "  select t2.b2, t2.c2, t11.a1 from --splice-properties joinOrder=fixed\n " +
                "t2, t11 where t2.b2=t11.b1 \n" +
                "  union all \n" +
                "  select t3.b3, t3.c3, t11.a1 from --splice-properties joinOrder=fixed\n " +
                "t3, t11 where t3.b3 = t11.b1) dt(Y, Z, W)  where Z=3 and Y=3 and W='C'", useSpark);

        /* plan should look like the following:
        Plan
        ----
        Cursor(n=19,rows=54,updateMode=READ_ONLY (1),engine=control)
          ->  ScrollInsensitive(n=18,totalCost=74.684,outputRows=54,outputHeapSize=72 B,partitions=1)
            ->  ProjectRestrict(n=17,totalCost=66.088,outputRows=54,outputHeapSize=72 B,partitions=1,preds=[(A1[22:3] = C         ),(SQLCol3[22:1] = 3),(SQLCol4[22:2] = 3)])
              ->  Union(n=16,totalCost=66.088,outputRows=54,outputHeapSize=72 B,partitions=1)
                ->  ProjectRestrict(n=15,totalCost=12.274,outputRows=18,outputHeapSize=72 B,partitions=1)
                  ->  BroadcastJoin(n=14,totalCost=12.274,outputRows=18,outputHeapSize=72 B,partitions=1,preds=[(T3.B3[19:1] = T11.B1[19:4])])
                    ->  TableScan[T11(4640)](n=13,totalCost=4.036,scannedRows=18,outputRows=18,outputHeapSize=72 B,partitions=1,preds=[(T11.A1[17:1] = C         )])
                    ->  TableScan[T3(4624)](n=12,totalCost=4.04,scannedRows=20,outputRows=17,outputHeapSize=34 B,partitions=1,preds=[(T3.C3[15:2] = 3),(T3.B3[15:1] = 3)])
                ->  Union(n=11,totalCost=41.178,outputRows=36,outputHeapSize=72 B,partitions=1)
                  ->  ProjectRestrict(n=10,totalCost=12.262,outputRows=18,outputHeapSize=72 B,partitions=1)
                    ->  MergeJoin(n=9,totalCost=12.262,outputRows=18,outputHeapSize=72 B,partitions=1,preds=[(T2.B2[11:1] = T11.B1[11:4])])
                      ->  TableScan[T11(4640)](n=8,totalCost=4.036,scannedRows=18,outputRows=18,outputHeapSize=72 B,partitions=1,preds=[(T11.A1[9:1] = C         )])
                      ->  ProjectRestrict(n=7,totalCost=4.028,outputRows=17,outputHeapSize=34 B,partitions=1)
                        ->  IndexScan[IDX_T2(4609)](n=6,totalCost=4.028,scannedRows=17,outputRows=17,outputHeapSize=34 B,partitions=1,baseTable=T2(4592),preds=[(T2.C2[7:1] = 3),(T2.B2[7:2] = 3)])
                  ->  ProjectRestrict(n=5,totalCost=12.262,outputRows=18,outputHeapSize=72 B,partitions=1)
                    ->  MergeJoin(n=4,totalCost=12.262,outputRows=18,outputHeapSize=72 B,partitions=1,preds=[(T1.B1[4:1] = T11.B1[4:4])])
                      ->  TableScan[T11(4640)](n=3,totalCost=4.036,scannedRows=18,outputRows=18,outputHeapSize=72 B,partitions=1,preds=[(T11.A1[2:1] = C         )])
                      ->  ProjectRestrict(n=2,totalCost=4.028,outputRows=17,outputHeapSize=34 B,partitions=1)
                        ->  IndexScan[IDX_T1(4577)](n=1,totalCost=4.028,scannedRows=17,outputRows=17,outputHeapSize=34 B,partitions=1,baseTable=T1(4560),preds=[(T1.C1[0:1] = 3),(T1.B1[0:2] = 3)])

        19 rows selected

         */
        rowContainsQuery(new int[]{7, 8, 12, 14, 17, 19}, "explain " + sqlText, methodWatcher,
                new String[]{"TableScan", "preds=[(T11.A1[17:1] = C         )]"},
                new String[]{"TableScan", "preds=[(T3.C3[15:2] = 3),(T3.B3[15:1] = 3)]"},
                new String[]{"TableScan", "preds=[(T11.A1[9:1] = C         )]"},
                new String[]{"IndexScan", "preds=[(T2.C2[7:1] = 3),(T2.B2[7:2] = 3)]"},
                new String[]{"TableScan", "preds=[(T11.A1[2:1] = C         )]"},
                new String[]{"IndexScan", "preds=[(T1.C1[0:1] = 3),(T1.B1[0:2] = 3)]"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "Y | Z | W |\n" +
                        "------------\n" +
                        " 3 | 3 | C |\n" +
                        " 3 | 3 | C |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testPredicateWithConstantExpression() throws Exception {
        String sqlText = format("select X, Y, Z from (\n" +
                "                  select c5, b5, a5 from t5 --splice-properties useSpark=%s \n" +
                "                     where b5=2 \n" +
                "                  union all \n" +
                "                  select c6, b6, a6 from t6 " +
                "                     where b6=2) dt(Z, Y, X) " +
                "                where X = substr('abcdeNNN', 1,5)  and Z = add_months('2018-10-01', 2)", useSpark);

        /* the plan should look like the following:
        Plan
        ----------------------------------------------------
        Cursor(n=9,rows=2,updateMode=READ_ONLY (1),engine=control)
          ->  ScrollInsensitive(n=8,totalCost=16.024,outputRows=2,outputHeapSize=3 B,partitions=1)
            ->  ProjectRestrict(n=7,totalCost=12.014,outputRows=2,outputHeapSize=3 B,partitions=1)
              ->  ProjectRestrict(n=6,totalCost=12.014,outputRows=2,outputHeapSize=3 B,partitions=1,preds=[(SQLCol4[6:1] = dataTypeServices: DATE ),(SQLCol6[6:3] = substring(abcdeNNN, 1, 5) )])
                ->  Union(n=5,totalCost=12.014,outputRows=2,outputHeapSize=3 B,partitions=1)
                  ->  ProjectRestrict(n=4,totalCost=4.002,outputRows=1,outputHeapSize=3 B,partitions=1)
                    ->  TableScan[T6(1744)](n=3,totalCost=4.002,scannedRows=1,outputRows=1,outputHeapSize=3 B,partitions=1,preds=[(A6[3:1] = substring(abcdeNNN, 1, 5) ),(B6[3:2] = 2),(C6[3:3] = dataTypeServices: DATE )])
                  ->  ProjectRestrict(n=2,totalCost=4.002,outputRows=1,outputHeapSize=3 B,partitions=1)
                    ->  TableScan[T5(1712)](n=1,totalCost=4.002,scannedRows=1,outputRows=1,outputHeapSize=3 B,partitions=1,preds=[(A5[0:1] = substring(abcdeNNN, 1, 5) ),(B5[0:2] = 2),(C5[0:3] = dataTypeServices: DATE )])
        9 rows selected
        */

        rowContainsQuery(new int[]{7, 9}, "explain " + sqlText, methodWatcher,
                new String[]{"TableScan", "preds=[(A6[3:1] = substring(abcdeNNN, 1, 5) ),(B6[3:2] = 2),(C6[3:3] = dataTypeServices: DATE )]"},
                new String[]{"TableScan", "preds=[(A5[0:1] = substring(abcdeNNN, 1, 5) ),(B5[0:2] = 2),(C5[0:3] = dataTypeServices: DATE )]"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "X   | Y |     Z     |\n" +
                        "-----------------------\n" +
                        "abcde | 2 |2018-12-01 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

    }

    //negative test cases
    @Test
    public void testPredicateWithColumnInExpression() throws Exception {
        /* predicate where expression on columns is not pushed down */
        String sqlText = format("select Y, Z from (select * from v1 left join t11 on Y=t11.b1) dt where Z+1=2 and Y=1", useSpark);

        /* plan should look like the following:
        Plan
        ----
        Cursor(n=10,rows=36,updateMode=READ_ONLY (1),engine=control)
          ->  ScrollInsensitive(n=9,totalCost=24.178,outputRows=36,outputHeapSize=132 B,partitions=2)
            ->  ProjectRestrict(n=8,totalCost=8.258,outputRows=36,outputHeapSize=132 B,partitions=2)
              ->  BroadcastLeftOuterJoin(n=7,totalCost=8.258,outputRows=36,outputHeapSize=132 B,partitions=2,preds=[(Y[10:1] = T11.B1[10:3])])
                ->  TableScan[T11(5760)](n=6,totalCost=4.04,scannedRows=20,outputRows=20,outputHeapSize=132 B,partitions=2)
                ->  ProjectRestrict(n=5,totalCost=12.253,outputRows=36,outputHeapSize=36 B,partitions=1,preds=[((V1.Z[6:2] + 1) = 2),(V1.Y[6:1] = 1)])
                  ->  Union(n=4,totalCost=12.253,outputRows=36,outputHeapSize=36 B,partitions=1)
                    ->  TableScan[T3(5744)](n=3,totalCost=4.04,scannedRows=20,outputRows=18,outputHeapSize=36 B,partitions=1,preds=[(B3[3:1] = 1)])
                    ->  ProjectRestrict(n=2,totalCost=4.033,outputRows=18,outputHeapSize=36 B,partitions=1)
                      ->  IndexScan[IDX_T2(5729)](n=1,totalCost=4.033,scannedRows=20,outputRows=18,outputHeapSize=36 B,partitions=1,baseTable=T2(5712),preds=[(B2[0:2] = 1)])

        10 rows selected
         */
        rowContainsQuery(new int[]{8, 10}, "explain " + sqlText, methodWatcher,
                new String[]{"preds=[(B3[3:1] = 1)]"},
                new String[]{"IndexScan", "preds=[(B2[0:2] = 1)]"});

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "Y | Z |\n" +
                        "--------\n" +
                        " 1 | 1 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testPredicatePushToUnionWithCharColumnOfDifferentLength() throws Exception {
        String[] constants = new String[]{"AAAAA", "BBB", "BBB  ", "cc", "cc "};
        String[] expectedRS = new String[]{
                "1    |\n" +
                        "---------\n" +
                        "-AAAAA- |",

                "",

                "1    |\n" +
                        "---------\n" +
                        "-BBB  - |",

                "",
                "1   |\n" +
                        "-------\n" +
                        "-cc - |"};
        for (int i = 0; i < constants.length; i++) {
            String sqlText = format("select '-' || a || '-' from (select a7 from t7 --splice-properties useSpark=%s\n " +
                    "union all select a8 from t8) dt(a) where a ='%s'", useSpark, constants[i]);
            ResultSet rs = methodWatcher.executeQuery(sqlText);

            assertEquals("\n" + sqlText + "\n", expectedRS[i], TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
        }

        // test constant expression
        String[] expectedRS1 = new String[]{
                "",
                "1   |\n" +
                        "-------\n" +
                        "-b  - |"};
        for (int i = 0; i < 2; i++) {
            String sqlText = format("select '-' || a || '-' from (select a7 from t7 --splice-properties useSpark=%s\n" +
                    "union all select a8 from t8) dt(a) where a = 'b' || repeat(' ', %d)", useSpark, i + 1);
            ResultSet rs = methodWatcher.executeQuery(sqlText);

            assertEquals("\n" + sqlText + "\n", expectedRS1[i], TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
        }
    }

    @Test
    public void testInlistPushToUnionWithCharColumnOfDifferentLength() throws Exception {
        String sqlText = format("select '-' || a || '-' from (select a7 from t7 --splice-properties useSpark=%s\n" +
                "union all select a8 from t8) dt(a) where a in ('AAAAA', 'aaa', 'BBB', 'b', 'CCCC ','cc ')", useSpark);
        String expected = "1    |\n" +
                "---------\n" +
                "-AAAAA- |\n" +
                "-CCCC - |\n" +
                " -aaa-  |\n" +
                " -cc -  |";
        ResultSet rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sqlText = format("select '-' || a || '-' from (select a7 from t7 --splice-properties useSpark=%s\n" +
                "union all select a8 from t8) dt(a) where a in ('BBB', 'BBB  ', 'b    ', 'b  ')", useSpark);
        expected = "1    |\n" +
                "---------\n" +
                "-BBB  - |\n" +
                " -b  -  |";
        rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // test inlist with expression
        sqlText = format("select '-' || a || '-' from (select a7 from t7 --splice-properties useSpark=%s\n" +
                "union all select a8 from t8) dt(a) " +
                "where a in (cast('BBB' as CHAR(3)), cast('BBB' as CHAR(5)), cast('b' as CHAR(3)), cast('b' as CHAR(5)))", useSpark);
        expected = "1    |\n" +
                "---------\n" +
                "-BBB  - |\n" +
                " -b  -  |";
        rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // test inlist on non-PK column
        sqlText = format("select '-' || b || '-' from (select b7 from t7 --splice-properties useSpark=%s\n" +
                "union all select b8 from t8) dt(b) where b in ('BBB', 'BBB  ', 'b    ', 'b  ')", useSpark);
        expected = "1    |\n" +
                "---------\n" +
                "-BBB  - |\n" +
                " -b  -  |";
        rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testPredicatePushToUnionWithVarcharColumnOfDifferentLength() throws Exception {
        String[] constants = new String[]{"AAAAA", "BBB", "BBB  ", "cc", "cc "};
        String[] expectedRS = new String[]{
                "1    |\n" +
                        "---------\n" +
                        "-AAAAA- |",

                "1   |\n" +
                        "-------\n" +
                        "-BBB- |",

                "1    |\n" +
                        "---------\n" +
                        "-BBB  - |",

                "1  |\n" +
                        "------\n" +
                        "-cc- |",
                ""};
        for (int i = 0; i < constants.length; i++) {
            String sqlText = format("select '-' || a || '-' from (select a9 from t9 --splice-properties useSpark=%s\n " +
                    "union all select a10 from t10) dt(a) where a ='%s'", useSpark, constants[i]);
            ResultSet rs = methodWatcher.executeQuery(sqlText);

            assertEquals("\n" + sqlText + "\n", expectedRS[i], TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
        }

        // test constant expression
        String[] expectedRS1 = new String[]{
                "1  |\n" +
                        "-----\n" +
                        "-b- |",
                "",
                "1   |\n" +
                        "-------\n" +
                        "-b  - |"};
        for (int i = 0; i <= 2; i++) {
            String sqlText = format("select '-' || a || '-' from (select a9 from t9 --splice-properties useSpark=%s\n" +
                    "union all select a10 from t10) dt(a) where a = 'b' || repeat(' ', %d)", useSpark, i);
            ResultSet rs = methodWatcher.executeQuery(sqlText);

            assertEquals("\n" + sqlText + "\n", expectedRS1[i], TestUtils.FormattedResult.ResultFactory.toString(rs));
            rs.close();
        }
    }

    @Test
    public void testInlistPushToUnionWithVarcharColumnOfDifferentLength() throws Exception {
        String sqlText = format("select '-' || a || '-' from (select a9 from t9 --splice-properties useSpark=%s\n" +
                "union all select a10 from t10) dt(a) where a in ('AAAAA', 'aaa', 'BBB', 'b', 'b  ', 'CCCC ','cc ')", useSpark);
        String expected = "1    |\n" +
                "---------\n" +
                "-AAAAA- |\n" +
                " -BBB-  |\n" +
                " -aaa-  |\n" +
                " -b  -  |\n" +
                "  -b-   |";
        ResultSet rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sqlText = format("select '-' || a || '-' from (select a9 from t9 --splice-properties useSpark=%s\n" +
                "union all select a10 from t10) dt(a) where a in ('BBB', 'BBB  ', 'b', 'b  ')", useSpark);
        expected = "1    |\n" +
                "---------\n" +
                "-BBB  - |\n" +
                " -BBB-  |\n" +
                " -b  -  |\n" +
                "  -b-   |";
        rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // test inlist with expression
        sqlText = format("select '-' || a || '-' from (select a9 from t9 --splice-properties useSpark=%s\n" +
                "union all select a10 from t10) dt(a) " +
                "where a in (cast('BBB' as CHAR(3)), cast('BBB' as CHAR(5)), cast('b' as CHAR(3)), cast('b' as CHAR(5)))", useSpark);
        expected = "1    |\n" +
                "---------\n" +
                "-BBB  - |\n" +
                " -BBB-  |\n" +
                " -b  -  |";
        rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // test inlist on non-PK column
        sqlText = format("select '-' || b || '-' from (select b9 from t9 --splice-properties useSpark=%s\n" +
                "union all select b10 from t10) dt(b) where b in ('BBB', 'BBB  ', 'b', 'b  ')", useSpark);
        expected = "1    |\n" +
                "---------\n" +
                "-BBB  - |\n" +
                " -BBB-  |\n" +
                " -b  -  |\n" +
                "  -b-   |";
        rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testUnsatisfiableEqualityConditionAfterPushToUnion() throws Exception {
        // case to rewrite to unsatisfiable condition if equality condition to a constant with column length that does not match
        // in this case branch 1 for the query to t7 will be pruned as a7 is of length 5 > len('BBB')
        String sqlText = format("select '-' || a || '-' from (select a7 from t7 --splice-properties useSpark=%s\n" +
                "union all select a8 from t8) dt(a) where a ='BBB'", useSpark);
        String expected = "";
        ResultSet rs = methodWatcher.executeQuery("explain  " + sqlText);
        String explainPlanText = TestUtils.FormattedResult.ResultFactory.toString(rs);
        Assert.assertTrue("Query is expected to be pruned but not", explainPlanText.contains("Values"));
        rs.close();

        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // negative case, rewrite does not happen for non-equality condition
        sqlText = format("select '-' || a || '-' from (select a7 from t7 --splice-properties useSpark=%s\n" +
                "union all select a8 from t8) dt(a) where a >'BBB'", useSpark);
        expected = "1    |\n" +
                "---------\n" +
                "-CCCC - |\n" +
                " -aaa-  |\n" +
                " -b  -  |\n" +
                " -cc -  |";
        rs = methodWatcher.executeQuery("explain  " + sqlText);
        explainPlanText = TestUtils.FormattedResult.ResultFactory.toString(rs);
        Assert.assertTrue("Query is not expected to be pruned", !explainPlanText.contains("Values"));
        rs.close();

        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }


    @Test
    public void testUnsatisfiableInlistConditionAfterPushToUnion() throws Exception {
        String sqlText = format("select '-' || a || '-' from (select a7 from t7 --splice-properties useSpark=%s\n" +
                "union all select a8 from t8) dt(a) where a in ('cc ', 'CCCC')", useSpark);
        String expected = "1   |\n" +
                "-------\n" +
                "-cc - |";
        ResultSet rs = methodWatcher.executeQuery("explain  " + sqlText);
        String explainPlanText = TestUtils.FormattedResult.ResultFactory.toString(rs);
        Assert.assertTrue("Query is expected to be pruned but not", explainPlanText.contains("Values"));
        rs.close();

        rs = methodWatcher.executeQuery(sqlText);
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testInlistWithParameterPushToUnionWithCharColumnOfDifferentLength() throws Exception {
        String sqlText = format("select '-' || a || '-' from (select a7 from t7 --splice-properties useSpark=%s\n" +
                "union all select a8 from t8) dt(a) where a in (?, ?, ?)", useSpark);
        PreparedStatement ps = methodWatcher.prepareStatement(sqlText);
        ps.setString(1, "AAAAA");
        ps.setString(2, "CCCC");
        ps.setString(3, "cc ");
        ResultSet rs = ps.executeQuery();
        String expected = "1    |\n" +
                "---------\n" +
                "-AAAAA- |\n" +
                " -cc -  |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // set parameters that can be pruned during execution stage
        ps.setString(1, "aaa");
        ps.setString(2, "b");
        ps.setString(3, "cc");
        rs = ps.executeQuery();
        expected = "1   |\n" +
                "-------\n" +
                "-aaa- |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testPredicateWithParameterPushToUnionWithVarCharColumnOfDifferentLength() throws Exception {
        String sqlText = format("select '-' || a || '-' from (select a9 from t9 --splice-properties useSpark=%s\n" +
                "union all select a10 from t10) dt(a) where a in (?, ?, ?)", useSpark);
        PreparedStatement ps = methodWatcher.prepareStatement(sqlText);
        ps.setString(1, "AAAAA");
        ps.setString(2, "CCCC");
        ps.setString(3, "cc ");
        ResultSet rs = ps.executeQuery();
        String expected = "1    |\n" +
                "---------\n" +
                "-AAAAA- |\n" +
                "-CCCC-  |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // set parameters that can be pruned during execution stage
        ps.setString(1, "AAAAA");
        ps.setString(2, "B   ");
        ps.setString(3, "CCCC");
        rs = ps.executeQuery();
        expected = "1    |\n" +
                "---------\n" +
                "-AAAAA- |\n" +
                "-CCCC-  |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // test inlist with elements different only on trailing spaces
        ps.setString(1, "b");
        ps.setString(2, "b ");
        ps.setString(3, "b  ");
        rs = ps.executeQuery();
        expected = "1   |\n" +
                "-------\n" +
                "-b  - |\n" +
                " -b-  |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    // test mixture branch
    @Test
    public void testPredicateWithParameterPushToUnionWithDifferentCharColumnTypes() throws Exception {
        String sqlText = format("select '-' || a || '-' from (select a9 from t9 --splice-properties useSpark=%s\n" +
                "union all select a8 from t8) dt(a) where a in (?, ?, ?)", useSpark);
        PreparedStatement ps = methodWatcher.prepareStatement(sqlText);
        ps.setString(1, "AAAAA");
        ps.setString(2, "CCCC");
        ps.setString(3, "cc ");
        ResultSet rs = ps.executeQuery();
        String expected = "1    |\n" +
                "---------\n" +
                "-AAAAA- |\n" +
                "-CCCC-  |\n" +
                " -cc -  |";
        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testJoinPredicatePushDownToUnion() throws Exception {
        // cross join cannot be used as the join strategy for the only table in a select block or
        // the left-most table in a join sequence

        // Q1: hint cross join for the left of the union
        String sqlText = format("select * from --splice-properties joinOrder=fixed\n" +
                "t7, (select a9, c9 from t9 --splice-properties joinStrategy=cross\n" +
                "union all select a10, c10 from t10  --splice-properties useSpark=%s\n" +
                ")dt(a,c) --splice-properties joinStrategy=nestedloop\n" +
                "where a=a7", useSpark);

        try {
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query should fail with no valid exeuction plan!");
        }catch (SQLException e) {
            Assert.assertTrue("Invalid exception thrown: " + e, e.getMessage().startsWith("No valid execution plan"));
        }

        // Q2: hint cross join for the right of the union
        sqlText = format("select * from --splice-properties joinOrder=fixed\n" +
                "t7, (select a9, c9 from t9 \n" +
                "union all select a10, c10 from t10 --splice-properties useSpark=%s, joinStrategy=cross\n" +
                ")dt(a,c) --splice-properties joinStrategy=nestedloop\n" +
                "where a=a7", useSpark);

        try {
            methodWatcher.executeQuery(sqlText);
            Assert.fail("Query should fail with no valid exeuction plan!");
        }catch (SQLException e) {
            Assert.assertTrue("Invalid exception thrown: " + e, e.getMessage().startsWith("No valid execution plan"));
        }

        // Q3: do not force cross join
        sqlText = format("select * from --splice-properties joinOrder=fixed\n" +
                "t7, (select a9, c9 from t9 \n" +
                "union all select a10, c10 from t10 --splice-properties useSpark=%s\n" +
                ")dt(a,c) --splice-properties joinStrategy=nestedloop\n" +
                "where a=a7", useSpark);
        String expected = "A7   | B7   |C7 |  A   | C |\n" +
                "-----------------------------\n" +
                "AAAAA |AAAAA | 1 |AAAAA | 1 |\n" +
                " BBB  | BBB  | 2 | BBB  |22 |";
        ResultSet rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }


    @Test
    public void testComparisionOfSQLCharAndVarchar() throws Exception {
        // test simple case of two table joining on SQLChar and SQLVarchar
        String sqlText = format("select * from t7, t9 --splice-properties joinStrategy=sortmerge, useSpark=%s\n" +
                "where a7=a9", useSpark);

        String expected = "A7   | B7   |C7 | A9   | B9   |C9 |\n" +
                "------------------------------------\n" +
                "AAAAA |AAAAA | 1 |AAAAA |AAAAA | 1 |\n" +
                " BBB  | BBB  | 2 | BBB  | BBB  |22 |";
        ResultSet rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        // hint dt with hashable join strategy
        sqlText = format("select * from --splice-properties joinOrder=fixed\n" +
                "t7, (select a9, c9 from t9 \n" +
                "union all select a10, c10 from t10 --splice-properties useSpark=%s\n" +
                ")dt(a,c) --splice-properties joinStrategy=broadcast\n" +
                "where a=a7", useSpark);

        expected = "A7   | B7   |C7 |  A   | C |\n" +
                "-----------------------------\n" +
                "AAAAA |AAAAA | 1 |AAAAA | 1 |\n" +
                " BBB  | BBB  | 2 | BBB  |22 |";

        rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        sqlText = format("select * from --splice-properties joinOrder=fixed\n" +
                "t7, (select a9, c9 from t9 \n" +
                "union all select a10, c10 from t10 --splice-properties useSpark=%s\n" +
                ")dt(a,c) --splice-properties joinStrategy=sortmerge\n" +
                "where a=a7", useSpark);

        rs = methodWatcher.executeQuery(sqlText);

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

    }
}