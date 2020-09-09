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

package com.splicemachine.db.impl.sql.compile;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.homeless.TestUtils;
import com.splicemachine.test_tools.TableCreator;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import splice.com.google.common.collect.Lists;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;
import static org.junit.Assert.assertEquals;

/**
 * Created by yxia on 8/19/20.
 */
@RunWith(Parameterized.class)
public class NLJPredicatePushedToDerivedTableIT extends SpliceUnitTest {

    private Boolean useSpark;

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{true});
        params.add(new Object[]{false});
        return params;
    }

    public static final String CLASS_NAME = NLJPredicatePushedToDerivedTableIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public NLJPredicatePushedToDerivedTableIT(Boolean useSpark) {
        this.useSpark = useSpark;
    }

    public static void createData(Connection conn, String schemaName) throws Exception {

        new TableCreator(conn)
                .withCreate("create table t1 (a1 int, b1 int, c1 int, primary key(a1))")
                .withInsert("insert into t1 values (?,?,?)")
                .withRows(rows(
                        row(1,1,1),
                        row(2,2,2),
                        row(3,3,3)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t2 (a2 int, b2 int, c2 int, primary key(a2))")
                .withInsert("insert into t2 values (?,?,?)")
                .withRows(rows(
                        row(1,1,1),
                        row(2,2,2),
                        row(4,4,4)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t3 (a3 int, b3 int, c3 int, primary key(a3))")
                .withInsert("insert into t3 values (?,?,?)")
                .withRows(rows(
                        row(1,1,1),
                        row(2,2,2),
                        row(5,5,5)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t4 (a4 int, b4 int, c4 int, primary key(a4))")
                .withInsert("insert into t4 values (?,?,?)")
                .withRows(rows(
                        row(1,1,1),
                        row(2,2,2),
                        row(5,5,5)))
                .create();

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s','T1', 3, 100, 1)",
                schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'T1', 'A1', 0, 1)",
                schemaName));
        conn.createStatement().executeQuery(format(
                "call syscs_util.fake_column_statistics('%s', 'T1', 'B1', 0, 1)",
                schemaName));

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s','T2', 10000, 100, 1)",
                schemaName));
        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_COLUMN_STATISTICS('%s','T2', 'A2', 0, 1)",
                schemaName));
        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_COLUMN_STATISTICS('%s','T2', 'B2', 0, 1)",
                schemaName));

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s','T3', 10000, 100, 1)",
                schemaName));
        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_COLUMN_STATISTICS('%s','T3', 'A3', 0, 1)",
                schemaName));
        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_COLUMN_STATISTICS('%s','T3', 'B3', 0, 1)",
                schemaName));

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s','T4', 1000, 100, 1)",
                schemaName));

        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_COLUMN_STATISTICS('%s','T4', 'A4', 0, 1)",
                schemaName));
        conn.createStatement().executeQuery(format(
                "CALL SYSCS_UTIL.FAKE_COLUMN_STATISTICS('%s','T4', 'B4', 0, 1)",
                schemaName));

        conn.commit();

    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        createData(spliceClassWatcher.getOrCreateConnection(), spliceSchemaWatcher.toString());
    }

    @Test
    public void testPushPredicateDownOneLevelOfDT() throws Exception {
        String sqlText = format("select a1 from \n" +
                "t1, (select a2 from t2, t4 where a2=a4) dt --splice-properties useSpark=%s\n" +
                "where a1=a2 and b1 in (1,2,3)", useSpark);

        /* plan should look like the following:
        Plan
        ----
        Cursor(n=13,rows=3,updateMode=READ_ONLY (1),engine=OLTP (default))
          ->  ScrollInsensitive(n=13,totalCost=112.303,outputRows=3,outputHeapSize=378 B,partitions=1)
            ->  ProjectRestrict(n=12,totalCost=100.252,outputRows=3,outputHeapSize=378 B,partitions=1)
              ->  NestedLoopJoin(n=10,totalCost=100.252,outputRows=3,outputHeapSize=378 B,partitions=1)
                ->  ProjectRestrict(n=8,totalCost=16.028,outputRows=1,outputHeapSize=66 B,partitions=1)
                  ->  NestedLoopJoin(n=6,totalCost=16.028,outputRows=1,outputHeapSize=66 B,partitions=1)
                    ->  TableScan[T4(1680)](n=4,totalCost=4.002,scannedRows=1,outputRows=1,outputHeapSize=66 B,partitions=1,preds=[(A2[3:1] = A4[4:1])])
                    ->  TableScan[T2(1648)](n=2,totalCost=4.002,scannedRows=1,outputRows=1,outputHeapSize=33 B,partitions=1,preds=[(A1[1:1] = A2[2:1])])
                ->  ProjectRestrict(n=1,totalCost=4.006,outputRows=3,outputHeapSize=180 B,partitions=1,preds=[(B1[0:2] IN (1,2,3))])
                  ->  TableScan[T1(1632)](n=0,totalCost=4.006,scannedRows=3,outputRows=3,outputHeapSize=180 B,partitions=1)

        10 rows selected
         */
        rowContainsQuery(new int[]{4, 6, 7, 8, 9, 10}, "explain " + sqlText, methodWatcher,
                new String[]{"NestedLoopJoin", "outputRows=3"},
                new String[]{"NestedLoopJoin", "outputRows=1"},
                new String[]{"TableScan[T4", "scannedRows=1,outputRows=1", "preds=[(A2[3:1] = A4[4:1])]"},
                new String[]{"TableScan[T2", "scannedRows=1,outputRows=1", "preds=[(A1[1:1] = A2[2:1])]"},
                new String[]{"ProjectRestrict", "outputRows=3", "preds=[(B1[0:2] IN (1,2,3))]"},
                new String[]{"TableScan[T1", "scannedRows=3,outputRows=3"}
                );

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 2 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testSessionProperty() throws Exception {
        TestConnection conn = methodWatcher.createConnection();
        conn.execute("set session_property DISABLE_NLJ_PREDICATE_PUSH_DOWN=true");

        String sqlText = format("select a1 from \n" +
                "t1, (select a2 from t2, t4 where a2=a4) dt --splice-properties useSpark=%s\n" +
                "where a1=a2 and b1 in (1,2,3)", useSpark);

        /* plan should look like the following:
        Plan
        ----
        Cursor(n=13,rows=8100,updateMode=READ_ONLY (1),engine=OLTP (default))
          ->  ScrollInsensitive(n=13,totalCost=11172.968,outputRows=8100,outputHeapSize=290.214 KB,partitions=1)
            ->  ProjectRestrict(n=12,totalCost=94.459,outputRows=8100,outputHeapSize=290.214 KB,partitions=1)
              ->  BroadcastJoin(n=10,totalCost=94.459,outputRows=8100,outputHeapSize=290.214 KB,partitions=1,preds=[(A1[10:2] = A2[10:1])])
                ->  ProjectRestrict(n=9,totalCost=4.006,outputRows=3,outputHeapSize=290.214 KB,partitions=1,preds=[(B1[8:2] IN (1,2,3))])
                  ->  TableScan[T1(1824)](n=8,totalCost=4.006,scannedRows=3,outputRows=3,outputHeapSize=290.214 KB,partitions=1)
                ->  ProjectRestrict(n=6,totalCost=54.325,outputRows=10000,outputHeapSize=358.072 KB,partitions=1)
                  ->  MergeJoin(n=4,totalCost=54.325,outputRows=10000,outputHeapSize=358.072 KB,partitions=1,preds=[(A2[4:1] = A4[4:2])])
                    ->  TableScan[T4(1872)](n=2,totalCost=6,scannedRows=1000,outputRows=1000,outputHeapSize=358.072 KB,partitions=1)
                    ->  TableScan[T2(1840)](n=0,totalCost=24,scannedRows=10000,outputRows=10000,outputHeapSize=325.521 KB,partitions=1)

        10 rows selected
         */
        rowContainsQuery(new int[]{4, 5, 6, 8, 9, 10}, "explain " + sqlText, conn,
                new String[]{"BroadcastJoin", "outputRows=8100", "preds=[(A1[10:2] = A2[10:1])]"},
                new String[]{"ProjectRestrict", "outputRows=3", "preds=[(B1[8:2] IN (1,2,3))]"},
                new String[]{"TableScan[T1", "scannedRows=3,outputRows=3"},
                new String[]{"MergeJoin", "outputRows=10000", "preds=[(A2[4:1] = A4[4:2])]"},
                new String[]{"TableScan[T4", "scannedRows=1000,outputRows=1000"},
                new String[]{"TableScan[T2", "scannedRows=10000,outputRows=10000"}
        );

        ResultSet rs = conn.query(sqlText);
        String expected =
                "A1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 2 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();

        conn.execute("set session_property DISABLE_NLJ_PREDICATE_PUSH_DOWN=false");
        conn.close();
    }

    @Test
    public void testDTWithOuterJoin() throws Exception {
        String sqlText = format("select a1 from \n" +
                "t1, (select a2 from t2 left join t4 on a2=a4) dt --splice-properties useSpark=%s\n" +
                "where a1=a2 and b1 in (1,2,3)", useSpark);

        /* plan should look like the following:
        Plan
        ----
        Cursor(n=13,rows=3,updateMode=READ_ONLY (1),engine=OLTP (default))
          ->  ScrollInsensitive(n=13,totalCost=112.353,outputRows=3,outputHeapSize=780 B,partitions=1)
            ->  ProjectRestrict(n=12,totalCost=100.295,outputRows=3,outputHeapSize=780 B,partitions=1)
              ->  NestedLoopJoin(n=10,totalCost=100.295,outputRows=3,outputHeapSize=780 B,partitions=1)
                ->  ProjectRestrict(n=8,totalCost=16.035,outputRows=1,outputHeapSize=200 B,partitions=1)
                  ->  NestedLoopLeftOuterJoin(n=6,totalCost=16.035,outputRows=1,outputHeapSize=200 B,partitions=1)
                    ->  TableScan[T4(1936)](n=4,totalCost=4.002,scannedRows=1,outputRows=1,outputHeapSize=200 B,partitions=1,preds=[(A2[3:1] = A4[4:1])])
                    ->  TableScan[T2(1904)](n=2,totalCost=4.002,scannedRows=1,outputRows=1,outputHeapSize=100 B,partitions=1,preds=[(A1[1:1] = A2[2:1])])
                ->  ProjectRestrict(n=1,totalCost=4.006,outputRows=3,outputHeapSize=180 B,partitions=1,preds=[(B1[0:2] IN (1,2,3))])
                  ->  TableScan[T1(1888)](n=0,totalCost=4.006,scannedRows=3,outputRows=3,outputHeapSize=180 B,partitions=1)

        10 rows selected
         */
        rowContainsQuery(new int[]{4, 6, 7, 8, 9, 10}, "explain " + sqlText, methodWatcher,
                new String[]{"NestedLoopJoin", "outputRows=3"},
                new String[]{"NestedLoopLeftOuterJoin", "outputRows=1"},
                new String[]{"TableScan[T4", "scannedRows=1,outputRows=1", "preds=[(A2[3:1] = A4[4:1])]"},
                new String[]{"TableScan[T2", "scannedRows=1,outputRows=1", "preds=[(A1[1:1] = A2[2:1])]"},
                new String[]{"ProjectRestrict", "outputRows=3", "preds=[(B1[0:2] IN (1,2,3))]"},
                new String[]{"TableScan[T1", "scannedRows=3,outputRows=3"}
        );

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 2 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testPushPredicateDownTwoLevelsOfDT() throws Exception {
        String sqlText = format("select * from\n" +
                                "t1, " +
                                "(select * from t2, (select X.a3 from t3 as X, t3 as Y where X.b3=Y.b3) dt where a2=dt.a3) dt2 --splice-properties useSpark=%s\n" +
                                "where a1=dt2.a3\n", useSpark);

        /* plan should look like the following:
        Plan
        ----
        Cursor(n=19,rows=3,updateMode=READ_ONLY (1),engine=OLTP (default))
          ->  ScrollInsensitive(n=19,totalCost=356.585,outputRows=3,outputHeapSize=897 B,partitions=1)
            ->  NestedLoopJoin(n=16,totalCost=340.517,outputRows=3,outputHeapSize=897 B,partitions=1)
              ->  ProjectRestrict(n=14,totalCost=56.07,outputRows=1,outputHeapSize=199 B,partitions=1)
                ->  NestedLoopJoin(n=12,totalCost=56.07,outputRows=1,outputHeapSize=199 B,partitions=1)
                  ->  TableScan[T2(2032)](n=10,totalCost=4.002,scannedRows=1,outputRows=1,outputHeapSize=199 B,partitions=1,preds=[(A2[10:1] = DT.A3[9:1])])
                  ->  ProjectRestrict(n=8,totalCost=36.029,outputRows=1,outputHeapSize=99 B,partitions=1)
                    ->  NestedLoopJoin(n=6,totalCost=36.029,outputRows=1,outputHeapSize=99 B,partitions=1)
                      ->  TableScan[T3(2048)](n=4,totalCost=24,scannedRows=10000,outputRows=1,outputHeapSize=99 B,partitions=1,preds=[(X.B3[3:2] = Y.B3[4:1])])
                      ->  TableScan[T3(2048)](n=2,totalCost=4.002,scannedRows=1,outputRows=1,outputHeapSize=66 B,partitions=1,preds=[(A1[1:1] = X.A3[2:1])])
              ->  TableScan[T1(2016)](n=0,totalCost=4.006,scannedRows=3,outputRows=3,outputHeapSize=300 B,partitions=1)

        11 rows selected
         */
        rowContainsQuery(new int[]{3, 5, 6, 8, 9, 10, 11}, "explain " + sqlText, methodWatcher,
                new String[]{"NestedLoopJoin", "outputRows=3"},
                new String[]{"NestedLoopJoin", "outputRows=1"},
                new String[]{"TableScan[T2", "scannedRows=1,outputRows=1", "preds=[(A2[10:1] = DT.A3[9:1])]"},
                new String[]{"NestedLoopJoin", "outputRows=1"},
                new String[]{"TableScan[T3", "scannedRows=10000,outputRows=1", "preds=[(X.B3[3:2] = Y.B3[4:1])]"},
                new String[]{"TableScan[T3", "scannedRows=1,outputRows=1", "preds=[(A1[1:1] = X.A3[2:1])]"},
                new String[]{"TableScan[T1", "scannedRows=3,outputRows=3"}
        );

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A1 |B1 |C1 |A2 |B2 |C2 |A3 |\n" +
                        "----------------------------\n" +
                        " 1 | 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                        " 2 | 2 | 2 | 2 | 2 | 2 | 2 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testJoinTwoDT() throws Exception {
        String sqlText = format("select * from\n" +
                " (select a1 from t1, t4 where a1=a4) dt1,\n" +
                " (select * from t2, t3 where a2=a3) dt2 --splice-properties useSpark=%s\n" +
                "where dt1.a1=dt2.a3", useSpark);

        /* plan should look like the following:
        Plan
        ----
        Cursor(n=19,rows=3,updateMode=READ_ONLY (1),engine=OLTP (default))
          ->  ScrollInsensitive(n=19,totalCost=168.536,outputRows=3,outputHeapSize=798 B,partitions=1)
            ->  NestedLoopJoin(n=16,totalCost=152.466,outputRows=3,outputHeapSize=798 B,partitions=1)
              ->  ProjectRestrict(n=14,totalCost=16.035,outputRows=1,outputHeapSize=200 B,partitions=1)
                ->  NestedLoopJoin(n=12,totalCost=16.035,outputRows=1,outputHeapSize=200 B,partitions=1)
                  ->  TableScan[T2(2160)](n=10,totalCost=4.002,scannedRows=1,outputRows=1,outputHeapSize=200 B,partitions=1,preds=[(A2[10:1] = A3[9:1])])
                  ->  TableScan[T3(2176)](n=8,totalCost=4.002,scannedRows=1,outputRows=1,outputHeapSize=100 B,partitions=1,preds=[(DT1.A1[7:1] = T3.A3[8:1])])
              ->  ProjectRestrict(n=6,totalCost=40.135,outputRows=3,outputHeapSize=198 B,partitions=1)
                ->  NestedLoopJoin(n=4,totalCost=40.135,outputRows=3,outputHeapSize=198 B,partitions=1)
                  ->  TableScan[T4(2192)](n=2,totalCost=4.002,scannedRows=1,outputRows=1,outputHeapSize=198 B,partitions=1,preds=[(A1[1:1] = A4[2:1])])
                  ->  TableScan[T1(2144)](n=0,totalCost=4.006,scannedRows=3,outputRows=3,outputHeapSize=99 B,partitions=1)

        11 rows selected

         */
        rowContainsQuery(new int[]{3, 5, 6, 7, 9, 10, 11}, "explain " + sqlText, methodWatcher,
                new String[]{"NestedLoopJoin", "outputRows=3"},
                new String[]{"NestedLoopJoin", "outputRows=1"},
                new String[]{"TableScan[T2", "scannedRows=1,outputRows=1", "preds=[(A2[10:1] = A3[9:1])]"},
                new String[]{"TableScan[T3", "scannedRows=1,outputRows=1", "preds=[(DT1.A1[7:1] = T3.A3[8:1])]"},
                new String[]{"NestedLoopJoin", "outputRows=3"},
                new String[]{"TableScan[T4", "scannedRows=1,outputRows=1", "preds=[(A1[1:1] = A4[2:1])]"},
                new String[]{"TableScan[T1", "scannedRows=3,outputRows=3"}
        );

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A1 |A2 |B2 |C2 |A3 |B3 |C3 |\n" +
                        "----------------------------\n" +
                        " 1 | 1 | 1 | 1 | 1 | 1 | 1 |\n" +
                        " 2 | 2 | 2 | 2 | 2 | 2 | 2 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testPushPredicateDownToRigtOfBroadcastJoin() throws Exception {
        String sqlText = format("select a1 from --splice-properties joinOrder=fixed\n" +
                "t1, (select * from t4, t2 --splice-properties joinStrategy=broadcast\n" +
                "     where a2=a4) dt --splice-properties joinStrategy=nestedloop, useSpark=%s\n" +
                "where a1=a2 and b1 in (1,2,3)", useSpark);

        /* plan should look like the following:
        Plan
        ----
        Cursor(n=13,rows=3000,updateMode=READ_ONLY (1),engine=OLTP (default))
          ->  ScrollInsensitive(n=13,totalCost=32279.342,outputRows=3000,outputHeapSize=273.533 KB,partitions=1)
            ->  ProjectRestrict(n=12,totalCost=24227.017,outputRows=3000,outputHeapSize=273.533 KB,partitions=1)
              ->  NestedLoopJoin(n=10,totalCost=24227.017,outputRows=3000,outputHeapSize=273.533 KB,partitions=1)
                ->  ProjectRestrict(n=8,totalCost=15.012,outputRows=1000,outputHeapSize=32.584 KB,partitions=1)
                  ->  BroadcastJoin(n=6,totalCost=15.012,outputRows=1000,outputHeapSize=32.584 KB,partitions=1,preds=[(A2[6:2] = A4[6:1])])
                    ->  TableScan[T2(2352)](n=4,totalCost=4.002,scannedRows=1,outputRows=1,outputHeapSize=32.584 KB,partitions=1,preds=[(A1[1:1] = T2.A2[4:1])])
                    ->  TableScan[T4(2384)](n=2,totalCost=6,scannedRows=1000,outputRows=1000,outputHeapSize=32.552 KB,partitions=1)
                ->  ProjectRestrict(n=1,totalCost=4.006,outputRows=3,outputHeapSize=180 B,partitions=1,preds=[(B1[0:2] IN (1,2,3))])
                  ->  TableScan[T1(2336)](n=0,totalCost=4.006,scannedRows=3,outputRows=3,outputHeapSize=180 B,partitions=1)

        10 rows selected
         */
        rowContainsQuery(new int[]{4, 6, 7, 8, 9, 10}, "explain " + sqlText, methodWatcher,
                new String[]{"NestedLoopJoin", "outputRows=3000"},
                new String[]{"BroadcastJoin", "outputRows=1000", "preds=[(A2[6:2] = A4[6:1])]"},
                new String[]{"TableScan[T2", "scannedRows=1,outputRows=1", "preds=[(A1[1:1] = T2.A2[4:1])]"},
                new String[]{"TableScan[T4", "scannedRows=1000,outputRows=1000"},
                new String[]{"ProjectRestrict", "outputRows=3", "preds=[(B1[0:2] IN (1,2,3))]"},
                new String[]{"TableScan[T1", "scannedRows=3,outputRows=3"}
        );

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A1 |\n" +
                        "----\n" +
                        " 1 |\n" +
                        " 2 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    //negative test cases
    @Test
    public void testExpressionNoPushDown() throws Exception {
        String sqlText = format("select a1 from --splice-properties joinOrder=fixed\n" +
                "t1, (select a2 from t2, t4 where b2=b4) dt --splice-properties joinStrategy=nestedloop, useSpark=%s\n" +
                "where a1+1=a2 and b1=1", useSpark);

        /* plan should look like the following:
        Plan
        ----
        Cursor(n=13,rows=10000,updateMode=READ_ONLY (1),engine=OLTP (default))
          ->  ScrollInsensitive(n=13,totalCost=80799.851,outputRows=10000,outputHeapSize=1.297 MB,partitions=1)
            ->  ProjectRestrict(n=12,totalCost=40446.091,outputRows=10000,outputHeapSize=1.297 MB,partitions=1)
              ->  NestedLoopJoin(n=10,totalCost=40446.091,outputRows=10000,outputHeapSize=1.297 MB,partitions=1)
                ->  ProjectRestrict(n=9,totalCost=40446.091,outputRows=10000,outputHeapSize=1.297 MB,partitions=1,preds=[((A1[1:1] + 1) = A2[8:1])])
                  ->  ProjectRestrict(n=8,totalCost=54.325,outputRows=10000,outputHeapSize=683.593 KB,partitions=1)
                    ->  BroadcastJoin(n=6,totalCost=54.325,outputRows=10000,outputHeapSize=683.593 KB,partitions=1,preds=[(B2[6:2] = B4[6:3])])
                      ->  TableScan[T4(2448)](n=4,totalCost=6,scannedRows=1000,outputRows=1000,outputHeapSize=683.593 KB,partitions=1)
                      ->  TableScan[T2(2416)](n=2,totalCost=24,scannedRows=10000,outputRows=10000,outputHeapSize=651.041 KB,partitions=1)
                ->  TableScan[T1(2400)](n=0,totalCost=4.006,scannedRows=3,outputRows=1,outputHeapSize=66 B,partitions=1,preds=[(B1[0:2] = 3)])

        10 rows selected
         */
        rowContainsQuery(new int[]{4, 5, 7, 8, 9, 10}, "explain " + sqlText, methodWatcher,
                new String[]{"NestedLoopJoin", "outputRows=10000"},
                new String[] {"ProjectRestrict", "outputRows=10000", "preds=[((A1[1:1] + 1) = A2[8:1])]"},
                new String[]{"BroadcastJoin", "outputRows=10000", "preds=[(B2[6:2] = B4[6:3])]"},
                new String[]{"TableScan[T4", "scannedRows=1000,outputRows=1000"},
                new String[]{"TableScan[T2", "scannedRows=10000,outputRows=10000"},
                new String[]{"TableScan[T1", "scannedRows=3,outputRows=1","preds=[(B1[0:2] = 1)]"}
        );

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A1 |\n" +
                        "----\n" +
                        " 1 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testExpressionNoPushDown2() throws Exception {
        String sqlText = format("select a1 from --splice-properties joinOrder=fixed\n" +
                "t1, (select a2+1 as A from t2, t4 where b2=b4) dt --splice-properties joinStrategy=nestedloop, useSpark=%s\n" +
                "where a1=A and b1=3", useSpark);

        /* plan should look like the following:
        Plan
        ----
        Cursor(n=13,rows=10000,updateMode=READ_ONLY (1),engine=OLTP (default))
          ->  ScrollInsensitive(n=13,totalCost=80799.851,outputRows=10000,outputHeapSize=1.297 MB,partitions=1)
            ->  ProjectRestrict(n=12,totalCost=40446.091,outputRows=10000,outputHeapSize=1.297 MB,partitions=1)
              ->  NestedLoopJoin(n=10,totalCost=40446.091,outputRows=10000,outputHeapSize=1.297 MB,partitions=1)
                ->  ProjectRestrict(n=9,totalCost=40446.091,outputRows=10000,outputHeapSize=1.297 MB,partitions=1,preds=[(A1[1:1] = A[8:1])])
                  ->  ProjectRestrict(n=8,totalCost=54.325,outputRows=10000,outputHeapSize=683.593 KB,partitions=1)
                    ->  BroadcastJoin(n=6,totalCost=54.325,outputRows=10000,outputHeapSize=683.593 KB,partitions=1,preds=[(B2[6:2] = B4[6:3])])
                      ->  TableScan[T4(2448)](n=4,totalCost=6,scannedRows=1000,outputRows=1000,outputHeapSize=683.593 KB,partitions=1)
                      ->  TableScan[T2(2416)](n=2,totalCost=24,scannedRows=10000,outputRows=10000,outputHeapSize=651.041 KB,partitions=1)
                ->  TableScan[T1(2400)](n=0,totalCost=4.006,scannedRows=3,outputRows=1,outputHeapSize=66 B,partitions=1,preds=[(B1[0:2] = 3)])

        10 rows selected
         */
        rowContainsQuery(new int[]{4, 5, 7, 8, 9, 10}, "explain " + sqlText, methodWatcher,
                new String[]{"NestedLoopJoin", "outputRows=10000"},
                new String[] {"ProjectRestrict", "outputRows=10000", "preds=[(A1[1:1] = A[8:1])]"},
                new String[]{"BroadcastJoin", "outputRows=10000", "preds=[(B2[6:2] = B4[6:3])]"},
                new String[]{"TableScan[T4", "scannedRows=1000,outputRows=1000"},
                new String[]{"TableScan[T2", "scannedRows=10000,outputRows=10000"},
                new String[]{"TableScan[T1", "scannedRows=3,outputRows=1","preds=[(B1[0:2] = 3)]"}
        );

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A1 |\n" +
                        "----\n" +
                        " 3 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testConstantExpressionNoPushDown() throws Exception {
        String sqlText = format("select a1 from --splice-properties joinOrder=fixed\n" +
                "t1, (select 2 as A from t2, t4 where b2=b4) dt --splice-properties joinStrategy=nestedloop, useSpark=%s\n" +
                "where a1=A and b1=2", useSpark);

        /* plan should look like the following:
        Plan
        ----
        Cursor(n=13,rows=10000,updateMode=READ_ONLY (1),engine=OLTP (default))
          ->  ScrollInsensitive(n=13,totalCost=80793.341,outputRows=10000,outputHeapSize=1002.604 KB,partitions=1)
            ->  ProjectRestrict(n=12,totalCost=40442.836,outputRows=10000,outputHeapSize=1002.604 KB,partitions=1)
              ->  NestedLoopJoin(n=10,totalCost=40442.836,outputRows=10000,outputHeapSize=1002.604 KB,partitions=1)
                ->  ProjectRestrict(n=9,totalCost=40442.836,outputRows=10000,outputHeapSize=1002.604 KB,partitions=1,preds=[(A1[1:1] = A[8:1])])
                  ->  ProjectRestrict(n=8,totalCost=54.325,outputRows=10000,outputHeapSize=358.072 KB,partitions=1)
                    ->  BroadcastJoin(n=6,totalCost=54.325,outputRows=10000,outputHeapSize=358.072 KB,partitions=1,preds=[(B2[6:1] = B4[6:2])])
                      ->  TableScan[T4(3088)](n=4,totalCost=6,scannedRows=1000,outputRows=1000,outputHeapSize=358.072 KB,partitions=1)
                      ->  TableScan[T2(3056)](n=2,totalCost=24,scannedRows=10000,outputRows=10000,outputHeapSize=325.521 KB,partitions=1)
                ->  TableScan[T1(3040)](n=0,totalCost=4.006,scannedRows=3,outputRows=1,outputHeapSize=66 B,partitions=1,preds=[(B1[0:2] = 2)])

        10 rows selected
         */
        rowContainsQuery(new int[]{4, 5, 7, 8, 9, 10}, "explain " + sqlText, methodWatcher,
                new String[]{"NestedLoopJoin", "outputRows=10000"},
                new String[] {"ProjectRestrict", "outputRows=10000", "preds=[(A1[1:1] = A[8:1])]"},
                new String[]{"BroadcastJoin", "outputRows=10000", "preds=[(B2[6:1] = B4[6:2])]"},
                new String[]{"TableScan[T4", "scannedRows=1000,outputRows=1000"},
                new String[]{"TableScan[T2", "scannedRows=10000,outputRows=10000"},
                new String[]{"TableScan[T1", "scannedRows=3,outputRows=1","preds=[(B1[0:2] = 2)]"}
        );

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A1 |\n" +
                        "----\n" +
                        " 2 |\n" +
                        " 2 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testAggregateExpressionNoPushDown() throws Exception {
        String sqlText = format("select a1 from --splice-properties joinOrder=fixed\n" +
                "t1, (select min(a2) as A from t2, t4 where b2=b4) dt --splice-properties joinStrategy=nestedloop, useSpark=%s\n" +
                "where a1=A and b1=1", useSpark);

        /* plan should look like the following:
        Plan
        ----
        Cursor(n=15,rows=10000,updateMode=READ_ONLY (1),engine=OLTP (default))
          ->  ScrollInsensitive(n=15,totalCost=80799.851,outputRows=10000,outputHeapSize=1.297 MB,partitions=1)
            ->  ProjectRestrict(n=14,totalCost=40446.091,outputRows=10000,outputHeapSize=1.297 MB,partitions=1)
              ->  NestedLoopJoin(n=12,totalCost=40446.091,outputRows=10000,outputHeapSize=1.297 MB,partitions=1)
                ->  ProjectRestrict(n=11,totalCost=40446.091,outputRows=10000,outputHeapSize=1.297 MB,partitions=1,preds=[(A1[1:1] = A[10:1])])
                  ->  ProjectRestrict(n=10,totalCost=40450.136,outputRows=1,outputHeapSize=0 B,partitions=1)
                    ->  GroupBy(n=9,totalCost=40450.136,outputRows=1,outputHeapSize=0 B,partitions=1)
                      ->  ProjectRestrict(n=8,totalCost=54.325,outputRows=10000,outputHeapSize=683.593 KB,partitions=1)
                        ->  BroadcastJoin(n=6,totalCost=54.325,outputRows=10000,outputHeapSize=683.593 KB,partitions=1,preds=[(B2[6:2] = B4[6:3])])
                          ->  TableScan[T4(3088)](n=4,totalCost=6,scannedRows=1000,outputRows=1000,outputHeapSize=683.593 KB,partitions=1)
                          ->  TableScan[T2(3056)](n=2,totalCost=24,scannedRows=10000,outputRows=10000,outputHeapSize=651.041 KB,partitions=1)
                ->  TableScan[T1(3040)](n=0,totalCost=4.006,scannedRows=3,outputRows=1,outputHeapSize=66 B,partitions=1,preds=[(B1[0:2] = 1)])

        12 rows selected
         */
        rowContainsQuery(new int[]{4, 5, 7, 9, 10, 11, 12, 13}, "explain " + sqlText, methodWatcher,
                new String[]{"NestedLoopJoin", "outputRows=10000"},
                new String[] {"ProjectRestrict", "outputRows=10000", "preds=[(A1[1:1] = A[10:1])]"},
                new String[] {"GroupBy"},
                new String[]{"BroadcastJoin", "outputRows=10000", "preds=[(B2[6:2] = B4[6:3])]"},
                new String[]{"TableScan[T4", "scannedRows=1000,outputRows=1000"},
                new String[]{"TableScan[T2", "scannedRows=10000,outputRows=10000"},
                new String[]{"TableScan[T1", "scannedRows=3,outputRows=1","preds=[(B1[0:2] = 1)]"}
        );

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A1 |\n" +
                        "----\n" +
                        " 1 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }

    @Test
    public void testSelectWithWindowFunctionNoPushDown() throws Exception {
        String sqlText = format("select a1 from --splice-properties joinOrder=fixed\n" +
                "t1, (select a2, row_number() over (order by a2 desc) as rnk from t2, t4 where b2=b4) dt --splice-properties joinStrategy=nestedloop, useSpark=%s\n" +
                "where a1=a2 and b1 in (1,2,3) and dt.rnk=1", useSpark);

        /* plan should look like the following:
        Plan
        ----
        Cursor(n=15,rows=30000,updateMode=READ_ONLY (1),engine=OLTP (default))
          ->  ScrollInsensitive(n=15,totalCost=162391.001,outputRows=30000,outputHeapSize=2.766 MB,partitions=1)
            ->  ProjectRestrict(n=14,totalCost=121860.496,outputRows=30000,outputHeapSize=2.766 MB,partitions=1)
              ->  NestedLoopJoin(n=12,totalCost=121860.496,outputRows=30000,outputHeapSize=2.766 MB,partitions=1)
                ->  ProjectRestrict(n=11,totalCost=121860.496,outputRows=30000,outputHeapSize=2.766 MB,partitions=1,preds=[(DT.RNK[10:2] = 1),(A1[1:1] = A2[10:1])])
                  ->  ProjectRestrict(n=10,totalCost=54.325,outputRows=10000,outputHeapSize=358.072 KB,partitions=1)
                    ->  WindowFunction(n=9,totalCost=6,outputRows=1000,outputHeapSize=358.072 KB,partitions=1)
                      ->  ProjectRestrict(n=8,totalCost=54.325,outputRows=10000,outputHeapSize=358.072 KB,partitions=1)
                        ->  MergeJoin(n=6,totalCost=54.325,outputRows=10000,outputHeapSize=358.072 KB,partitions=1,preds=[(A2[6:1] = A4[6:2])])
                          ->  TableScan[T4(1728)](n=4,totalCost=6,scannedRows=1000,outputRows=1000,outputHeapSize=358.072 KB,partitions=1)
                          ->  TableScan[T2(1696)](n=2,totalCost=24,scannedRows=10000,outputRows=10000,outputHeapSize=325.521 KB,partitions=1)
                ->  ProjectRestrict(n=1,totalCost=4.006,outputRows=3,outputHeapSize=180 B,partitions=1,preds=[(B1[0:2] IN (1,2,3))])
                  ->  TableScan[T1(1680)](n=0,totalCost=4.006,scannedRows=3,outputRows=3,outputHeapSize=180 B,partitions=1)

        13 rows selected
         */
        rowContainsQuery(new int[]{4, 5}, "explain " + sqlText, methodWatcher,
                new String[]{"NestedLoopJoin"},
                new String[] {"ProjectRestrict", "preds=[(DT.RNK[10:2] = 1),(A1[1:1] = A2[10:1])]"}
        );

        ResultSet rs = methodWatcher.executeQuery(sqlText);
        String expected =
                "A1 |\n" +
                        "----\n" +
                        " 2 |";

        assertEquals("\n" + sqlText + "\n", expected, TestUtils.FormattedResult.ResultFactory.toString(rs));
        rs.close();
    }
}
