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

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceUnitTest;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import com.splicemachine.test.HBaseTest;
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
import java.sql.SQLSyntaxErrorException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.Collection;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

@Category(HBaseTest.class)
@RunWith(Parameterized.class)
public class SparkExplainIT extends SpliceUnitTest {
    private static Logger LOG = Logger.getLogger(SparkExplainIT.class);
    public static final String CLASS_NAME = SparkExplainIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @Parameterized.Parameters
    public static Collection<Object[]> data() {
        Collection<Object[]> params = Lists.newArrayListWithCapacity(2);
        params.add(new Object[]{"true"});
        params.add(new Object[]{"false"});
        return params;
    }

    private String useSpark;

    public SparkExplainIT(String useSpark) {
        this.useSpark = useSpark;
    }

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    public static void createData(Connection conn, String schemaName) throws Exception {
        new TableCreator(conn)
                .withCreate("create table t1(a1 int not null, b1 int, c1 int, primary key(c1))")
                .withInsert("insert into t1 values(?,?,?)")
                .withRows(rows(
                        row(1,10,1),
                        row(2,20,2),
                        row(2,20,3),
                        row(3,30,4),
                        row(4,40,5)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t11(a1 int not null, b1 int, c1 int)")
                .withIndex("create index idx_t11 on t11(b1)")
                .withInsert("insert into t11 values(?,?,?)")
                .withRows(rows(
                        row(1,10,1),
                        row(2,20,2),
                        row(2,20,3),
                        row(3,30,4),
                        row(4,40,5)))
                .create();

        new TableCreator(conn)
                .withCreate("create table t2 (a2 int not null, b2 int, c2 int, primary key(c2))")
                .withInsert("insert into t2 values(?,?,?)")
                .withRows(rows(
                        row(2, 20, 1),
                        row(2, 20, 2),
                        row(3, 30, 3),
                        row(4, 40, 4),
                        row(5, 50, 5),
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

        new TableCreator(conn)
                .withCreate("create table t7 (a7 int not null, b7 date, c7 varchar(5), primary key (a7))")
                .withInsert("insert into t7 values(?,?,?)")
                .withRows(rows(
                        row(2, "2020-01-02", "20A"),
                        row(3, "2020-01-03", "30A"),
                        row(7, "2020-01-07", null),
                        row(8, "2020-01-08", "80A")))
                .create();

        // Create TPC-H Tables.
        new TableCreator(conn)
                .withCreate("CREATE TABLE LINEITEM (\n" +
                " L_ORDERKEY BIGINT NOT NULL,\n" +
                " L_PARTKEY INTEGER NOT NULL,\n" +
                " L_SUPPKEY INTEGER NOT NULL,\n" +
                " L_LINENUMBER INTEGER NOT NULL,\n" +
                " L_QUANTITY DECIMAL(15,2),\n" +
                " L_EXTENDEDPRICE DECIMAL(15,2),\n" +
                " L_DISCOUNT DECIMAL(15,2),\n" +
                " L_TAX DECIMAL(15,2),\n" +
                " L_RETURNFLAG VARCHAR(1),\n" +
                " L_LINESTATUS VARCHAR(1),\n" +
                " L_SHIPDATE DATE,\n" +
                " L_COMMITDATE DATE,\n" +
                " L_RECEIPTDATE DATE,\n" +
                " L_SHIPINSTRUCT VARCHAR(25),\n" +
                " L_SHIPMODE VARCHAR(10),\n" +
                " L_COMMENT VARCHAR(44),\n" +
                " PRIMARY KEY(L_ORDERKEY,L_LINENUMBER)\n" +
                " )")
                .withIndex("create index L_PART_IDX_TEMP on lineitem(L_PARTKEY, L_ORDERKEY, L_SUPPKEY, L_SHIPDATE, L_EXTENDEDPRICE, L_DISCOUNT, L_QUANTITY, L_SHIPMODE)")
                .withIndex("create index L_SHIPDATE_IDX_TEMP on lineitem(L_SHIPDATE, L_PARTKEY, L_EXTENDEDPRICE, L_DISCOUNT)")
                .create();

        new TableCreator(conn)
                .withCreate("CREATE TABLE ORDERS (\n" +
                " O_ORDERKEY BIGINT NOT NULL PRIMARY KEY,\n" +
                " O_CUSTKEY INTEGER,\n" +
                " O_ORDERSTATUS VARCHAR(1),\n" +
                " O_TOTALPRICE DECIMAL(15,2),\n" +
                " O_ORDERDATE DATE,\n" +
                " O_ORDERPRIORITY VARCHAR(15),\n" +
                " O_CLERK VARCHAR(15),\n" +
                " O_SHIPPRIORITY INTEGER ,\n" +
                " O_COMMENT VARCHAR(79)\n" +
                " )")
                .create();

        new TableCreator(conn)
                .withCreate("CREATE TABLE CUSTOMER (\n" +
                " C_CUSTKEY INTEGER NOT NULL PRIMARY KEY,\n" +
                " C_NAME VARCHAR(25),\n" +
                " C_ADDRESS VARCHAR(40),\n" +
                " C_NATIONKEY INTEGER NOT NULL,\n" +
                " C_PHONE VARCHAR(15),\n" +
                " C_ACCTBAL DECIMAL(15,2),\n" +
                " C_MKTSEGMENT VARCHAR(10),\n" +
                " C_COMMENT VARCHAR(117)\n" +
                " )")
                .create();

        new TableCreator(conn)
                .withCreate("CREATE TABLE PARTSUPP (\n" +
                " PS_PARTKEY INTEGER NOT NULL ,\n" +
                " PS_SUPPKEY INTEGER NOT NULL ,\n" +
                " PS_AVAILQTY INTEGER,\n" +
                " PS_SUPPLYCOST DECIMAL(15,2),\n" +
                " PS_COMMENT VARCHAR(199),\n" +
                " PRIMARY KEY(PS_PARTKEY,PS_SUPPKEY)\n" +
                " )")
                .create();

        new TableCreator(conn)
                .withCreate("CREATE TABLE SUPPLIER (\n" +
                " S_SUPPKEY INTEGER NOT NULL PRIMARY KEY,\n" +
                " S_NAME VARCHAR(25) ,\n" +
                " S_ADDRESS VARCHAR(40) ,\n" +
                " S_NATIONKEY INTEGER ,\n" +
                " S_PHONE VARCHAR(15) ,\n" +
                " S_ACCTBAL DECIMAL(15,2),\n" +
                " S_COMMENT VARCHAR(101)\n" +
                " )")
                .create();

        new TableCreator(conn)
                .withCreate("CREATE TABLE PART (\n" +
                " P_PARTKEY INTEGER NOT NULL PRIMARY KEY,\n" +
                " P_NAME VARCHAR(55) ,\n" +
                " P_MFGR VARCHAR(25) ,\n" +
                " P_BRAND VARCHAR(10) ,\n" +
                " P_TYPE VARCHAR(25) ,\n" +
                " P_SIZE INTEGER ,\n" +
                " P_CONTAINER VARCHAR(10) ,\n" +
                " P_RETAILPRICE DECIMAL(15,2),\n" +
                " P_COMMENT VARCHAR(23)\n" +
                " )")
                .create();

        new TableCreator(conn)
                .withCreate("CREATE TABLE REGION (\n" +
                " R_REGIONKEY INTEGER NOT NULL PRIMARY KEY,\n" +
                " R_NAME VARCHAR(25),\n" +
                " R_COMMENT VARCHAR(152)\n" +
                " )")
                .create();

        new TableCreator(conn)
                .withCreate("CREATE TABLE NATION (\n" +
                " N_NATIONKEY INTEGER NOT NULL,\n" +
                " N_NAME VARCHAR(25),\n" +
                " N_REGIONKEY INTEGER NOT NULL,\n" +
                " N_COMMENT VARCHAR(152),\n" +
                " PRIMARY KEY (N_NATIONKEY)\n" +
                " )")
                .create();

        new TableCreator(conn)
                .withCreate("CREATE TABLE BIG (i int)")
                .create();

        conn.commit();
    }

    @BeforeClass
    public static void createDataSet() throws Exception {
        TestConnection conn = spliceClassWatcher.getOrCreateConnection();
        String schema = spliceSchemaWatcher.toString();
        createData(conn, schema);
        try (Statement st = conn.createStatement()) {
            st.execute(String.format("CALL SYSCS_UTIL.FAKE_TABLE_STATISTICS('%s', 'BIG', 100000, 100, 20)", schema));
        }
        conn.commit();
    }

    @Test
    public void testValues() throws Exception {
        String sqlText = "sparkexplain values 1";
        testQueryContains(sqlText, Arrays.asList("values", "ScrollInsensitive"), methodWatcher, true);
    }

    @Test
    public void testInsert() throws Exception {
        String sqlText = "sparkexplain insert into t1 select * from t2";

        testQueryContains(sqlText, "insert", methodWatcher, true);

        sqlText = "select count(*) from t1";
        String expected = "1 |\n" +
        "----\n" +
        " 5 |";
        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testUpdate() throws Exception {
        String sqlText = "sparkexplain update t1 set a1=0";

        testQueryContains(sqlText, "update", methodWatcher, true);

        sqlText = "select count(*) from t1 where a1 <> 0";
        String expected = "1 |\n" +
        "----\n" +
        " 5 |";
        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testDelete() throws Exception {
        String sqlText = "sparkexplain delete from t1";

        testQueryContains(sqlText, "delete", methodWatcher, true);

        sqlText = "select count(*) from t1";
        String expected = "1 |\n" +
        "----\n" +
        " 5 |";
        testQuery(sqlText, expected, methodWatcher);
    }

    @Test
    public void testLimit() throws Exception {
        String sqlText = "sparkexplain select top 2 * from t1";

        testQueryContains(sqlText, "limit", methodWatcher, true);
    }

    @Test
    public void testExport() throws Exception {
        String sqlText = "sparkexplain EXPORT('/my/export/dir', false, null, null, null, null)\n" +
        "          SELECT * FROM t1";

        try {
            // Explain on an EXPORT is a syntax error.
            testQueryContains(sqlText, "TheQueryShouldHitASyntaxError", methodWatcher, true);
        }
        catch (SQLSyntaxErrorException e) {

        }
    }

    @Test
    public void testImport() throws Exception {
        String sqlText = "sparkexplain CALL SYSCS_UTIL.IMPORT_DATA('SPLICEBBALL', 'Players',\n" +
        "    'ID, Team, Name, Position, DisplayName, BirthDate',\n" +
        "    '/Data/DocExamplesDb/Players.csv',\n" +
        "    null, null, null, null, null, 0, null, true, null)";

        // This is a No-Op, so this statement should not execute and we
        // should not see a "Table x does not exist" error.
        testQueryDoesNotContain(sqlText, "does not exist", methodWatcher, true);
    }

    @Test
    public void testBulkImport() throws Exception {
        String sqlText = "sparkexplain call SYSCS_UTIL.BULK_IMPORT_HFILE('TPCH', 'LINEITEM', null,\n" +
        "            's3a://splice-benchmark-data/flat/TPCH/1/lineitem', '|', null, null, null, null, -1,\n" +
        "            '/BAD', true, null, 'hdfs:///tmp/test_hfile_import/', false)";

        // This is a No-Op, so this statement should not execute and we
        // should not see a "Table x does not exist" error.
        testQueryDoesNotContain(sqlText, "does not exist", methodWatcher, true);
    }

    @Test
    public void testAggregation() throws Exception {
        String sqlText = format("sparkexplain select SUM(a1) from t1 inner join t2 --splice-properties useSpark=%s\n on c1=c2", useSpark);

        //Native Spark Execution Plan
        //----------------------------
        //-> NativeSparkDataSet
        //     *HashAggregate(keys=[], functions=[sum(cast(c1#71821 as bigint))], output=[c0#71856L])
        //     +- Exchange SinglePartition
        //        +- *HashAggregate(keys=[], functions=[partial_sum(cast(c1#71821 as bigint))], output=[sum#71860L])
        //           +- *Project [c0#71771 AS c1#71821]
        //              +- *BroadcastHashJoin [c1#71772], [c0#71782], Inner, BuildRight
        //                 :- *Filter isnotnull(c1#71772)
        //                 :  +- Scan ExistingRDD[c0#71771,c1#71772]
        //                       -> TableScan[T1(8064)](RS=0,totalCost=4.04, scannedRows=20,outputRows=20, outputHeapSize=60 B,partitions=1)
        //                 +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)))
        //                    +- *Filter isnotnull(c0#71782)
        //                       +- Scan ExistingRDD[c0#71782]
        //                          -> TableScan[T2(8080)](RS=2,totalCost=4.04, scannedRows=20,outputRows=20, outputHeapSize=108 B,partitions=1)

        testQueryContains(sqlText, "join", methodWatcher, true);
        testQueryContains(sqlText, Arrays.asList("GroupBy", "partial_sum"), methodWatcher, true);
    }

    @Test
    public void testOtherSparkExplainFlavors() throws Exception {
        String sqlText = format("sparkexplain_logical select SUM(a1) from t1 inner join t2 --splice-properties useSpark=%s\n on c1=c2", useSpark);
        //Native Spark Logical Plan
        //--------------------------
        //-> NativeSparkDataSet
        //     Project [c0#71583L AS c0#71586L]
        //     +- Project [c0#71558L AS c0#71583L]
        //        +- Project [c0#71558L]
        //           +- Project [c0#71558L, c1#71568, c2#71572]
        //              +- Project [c0#71558L, c1#71568, cast(null as binary) AS c2#71572]
        //                 +- Project [c0#71558L, cast(null as int) AS c1#71568]
        //                    +- Aggregate [cast(sum(cast(c1#71551 as bigint)) as bigint) AS c0#71558L]
        //                       +- Project [c1#71551]
        //                          +- Project [c1#71551, c2#71552]
        //                             +- Project [c0#71543L AS c0#71550L, c1#71544 AS c1#71551, c2#71545 AS c2#71552]
        //                                +- Project [cast(null as bigint) AS c0#71543L, c0#71536 AS c1#71544, null AS c2#71545]
        //                                   +- Project [c0#71529 AS c0#71536, c1#71530 AS c1#71537, c2#71531 AS c2#71538]
        //                                      +- Project [c0#71506 AS c0#71529, c1#71507 AS c1#71530, c0#71512 AS c2#71531]
        //                                         +- Join Inner, (c1#71507 = c0#71512)
        //                                            :- Project [c0#71501 AS c0#71506, c1#71502 AS c1#71507]
        //                                            :  +- LogicalRDD [c0#71501, c1#71502]
        //                                                  -> TableScan[T1(8064)](RS=0,totalCost=4.04, scannedRows=20,outputRows=20, outputHeapSize=60 B,partitions=1)
        //                                            +- ResolvedHint isBroadcastable=true
        //                                               +- LogicalRDD [c0#71512]
        //                                                  -> TableScan[T2(8080)](RS=2,totalCost=4.04, scannedRows=20,outputRows=20, outputHeapSize=108 B,partitions=1)

        testQueryContains(sqlText, "join", methodWatcher, true);
        testQueryContains(sqlText, Arrays.asList("GroupBy", "Aggregate"), methodWatcher, true);

        sqlText = format("sparkexplain_analyzed select SUM(a1) from t1 inner join t2 --splice-properties useSpark=%s\n on c1=c2", useSpark);

        //Native Spark Analyzed Plan
        //---------------------------
        //-> NativeSparkDataSet
        //     Project [c0#71673L AS c0#71676L]
        //     +- Project [c0#71648L AS c0#71673L]
        //        +- Project [c0#71648L]
        //           +- Project [c0#71648L, c1#71658, c2#71662]
        //              +- Project [c0#71648L, c1#71658, cast(null as binary) AS c2#71662]
        //                 +- Project [c0#71648L, cast(null as int) AS c1#71658]
        //                    +- Aggregate [cast(sum(cast(c1#71641 as bigint)) as bigint) AS c0#71648L]
        //                       +- Project [c1#71641]
        //                          +- Project [c1#71641, c2#71642]
        //                             +- Project [c0#71633L AS c0#71640L, c1#71634 AS c1#71641, c2#71635 AS c2#71642]
        //                                +- Project [cast(null as bigint) AS c0#71633L, c0#71626 AS c1#71634, null AS c2#71635]
        //                                   +- Project [c0#71619 AS c0#71626, c1#71620 AS c1#71627, c2#71621 AS c2#71628]
        //                                      +- Project [c0#71596 AS c0#71619, c1#71597 AS c1#71620, c0#71602 AS c2#71621]
        //                                         +- Join Inner, (c1#71597 = c0#71602)
        //                                            :- Project [c0#71591 AS c0#71596, c1#71592 AS c1#71597]
        //                                            :  +- LogicalRDD [c0#71591, c1#71592]
        //                                                  -> TableScan[T1(8064)](RS=0,totalCost=4.04, scannedRows=20,outputRows=20, outputHeapSize=60 B,partitions=1)
        //                                            +- ResolvedHint isBroadcastable=true
        //                                               +- LogicalRDD [c0#71602]
        //                                                  -> TableScan[T2(8080)](RS=2,totalCost=4.04, scannedRows=20,outputRows=20, outputHeapSize=108 B,partitions=1)

        testQueryContains(sqlText, "join", methodWatcher, true);
        testQueryContains(sqlText, Arrays.asList("GroupBy", "Aggregate"), methodWatcher, true);

        sqlText = format("sparkexplain_optimized select SUM(a1) from t1 inner join t2 --splice-properties useSpark=%s\n on c1=c2", useSpark);

        //Native Spark Optimized Plan
        //----------------------------
        //-> NativeSparkDataSet
        //     Aggregate [sum(cast(c1#71731 as bigint)) AS c0#71766L]
        //     +- Project [c0#71681 AS c1#71731]
        //        +- Join Inner, (c1#71682 = c0#71692)
        //           :- Filter isnotnull(c1#71682)
        //           :  +- LogicalRDD [c0#71681, c1#71682]
        //                 -> TableScan[T1(8064)](RS=0,totalCost=4.04, scannedRows=20,outputRows=20, outputHeapSize=60 B,partitions=1)
        //           +- ResolvedHint isBroadcastable=true
        //              +- Filter isnotnull(c0#71692)
        //                 +- LogicalRDD [c0#71692]
        //                    -> TableScan[T2(8080)](RS=2,totalCost=4.04, scannedRows=20,outputRows=20, outputHeapSize=108 B,partitions=1)

        testQueryContains(sqlText, "join", methodWatcher, true);
        testQueryContains(sqlText, Arrays.asList("GroupBy", "Aggregate"), methodWatcher, true);
    }

    @Test
    public void testJoins() throws Exception {
        String sqlText = format("sparkexplain select SUM(a1) from t1 inner join t2 --splice-properties joinStrategy=MERGE, useSpark=%s\n on c1=c2", useSpark);

        testQueryContains(sqlText, "MergeJoin", methodWatcher, true);
        sqlText = format("sparkexplain select SUM(a1) from --splice-properties joinOrder=fixed\n t1, t2 --splice-properties joinStrategy=NESTEDLOOP, useSpark=%s\n ", useSpark);
        testQueryContains(sqlText, "NestedLoopJoin", methodWatcher, true);
        sqlText = format("sparkexplain select SUM(a1) from --splice-properties joinOrder=fixed\n t1, t2 --splice-properties joinStrategy=CROSS, useSpark=%s\n ", useSpark);
        testQueryContains(sqlText, Arrays.asList("CrossJoin", "NestedLoopJoin", "CARTESIANPRODUCT"), methodWatcher, true);
        sqlText = format("sparkexplain select SUM(a1) from --splice-properties joinOrder=fixed\n t1, t2 --splice-properties joinStrategy=BROADCAST, useSpark=%s\n WHERE a1=a2", useSpark);
        testQueryContains(sqlText, "BroadCast", methodWatcher, true);
        sqlText = format("sparkexplain select SUM(a1) from --splice-properties joinOrder=fixed\n t1, t2 --splice-properties joinStrategy=SORTMERGE, useSpark=%s\n WHERE a1=a2", useSpark);
        testQueryContains(sqlText, Arrays.asList("SortMerge", "MergeSort"), methodWatcher, true);
        sqlText = format("sparkexplain select SUM(a1) from --splice-properties joinOrder=fixed\n t1, t2 --splice-properties joinStrategy=MERGE, useSpark=%s\n WHERE c1=c2", useSpark);
        testQueryContains(sqlText, "MergeJoin", methodWatcher, true);
    }

    @Test
    public void testAggregations() throws Exception {
        String sqlText = format("sparkexplain select SUM(a1) from t1 inner join t2 --splice-properties useSpark=%s\n on c1=c2 group by c2", useSpark);
        testQueryContains(sqlText, Arrays.asList("HASHAGGREGATE", "GroupBy"), methodWatcher, true);

        sqlText = format("sparkexplain select Count(distinct a1) from t1 inner join t2 --splice-properties useSpark=%s\n on c1=c2 group by c2", useSpark);
        testQueryContains(sqlText, Arrays.asList("Distinct", "GroupBy"), methodWatcher, true);

        sqlText = format("sparkexplain select Count(distinct a1) from t1 inner join t2 --splice-properties useSpark=%s\n on c1=c2", useSpark);
        testQueryContains(sqlText, Arrays.asList("Distinct", "GroupBy"), methodWatcher, true);
    }

    @Test
    public void simpleTwoTableFullJoined() throws Exception {
        String sqlText = format("sparkexplain select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s\n on c1=c2", useSpark);
        String expected = "fullouter";

        testQueryContains(sqlText, expected, methodWatcher, true);
    }

    @Test
    public void twoTableFullJoinedWithBothEqualityAndNonEqualityCondition() throws Exception {
        String sqlText = format("sparkexplain select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s\n on c1+1=c2 and a1<a2", useSpark);
        String expected = "fullouter";
        testQueryContains(sqlText, expected, methodWatcher, true);
    }

    @Test
    public void twoTableFullJoinedThroughRDDImplementation() throws Exception {
        String sqlText = format("sparkexplain select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s\n on a1=a2 and case when c1=2 then 2 end=c2", useSpark);
        String expected = "fullouter";
        testQueryContains(sqlText, expected, methodWatcher, true);
    }

    @Test
    public void fullJoinWithATableWithSingleTableCondition() throws Exception {
        String sqlText = format("sparkexplain select b1, a1, b2, a2, c2 from t1 full join (select * from t2 --splice-properties useSpark=%s\n " +
                "where a2=3) dt on a1=a2", useSpark);
        String expected = "fullouter";
        testQueryContains(sqlText, expected, methodWatcher, true);
    }

    @Test
    public void fullJoinWithInEqualityCondition() throws Exception {
        String sqlText = format("sparkexplain select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s\n on a1>a2", useSpark);
        String expected = "fullouter";
        testQueryContains(sqlText, expected, methodWatcher, true);
    }

    @Test
    public void fullJoinWithInEqualityConditionThroughRDDImplementation() throws Exception {
        String sqlText = format("sparkexplain select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s\n on a1>a2 and case when c1=3 then 3 end>c2", useSpark);
        String expected = "fullouter";
        testQueryContains(sqlText, expected, methodWatcher, true);
    }

    @Test
    public void testConsecutiveFullOuterJoins() throws Exception {
        String sqlText = format("sparkexplain select * from t1 full join t2 --splice-properties useSpark=%s\n" +
                "full join t3 " +
                "on a2=a3 on a1=a3", useSpark);
        String expected = "fullouter";
        testQueryContains(sqlText, expected, methodWatcher, true);
    }

    @Test
    public void testMixtureOfOuterJoins() throws Exception {
        String sqlText = format("sparkexplain select * from t1 left join t2 --splice-properties useSpark=%s, useDefaultRowCount=1000000000\n" +
                "full join t3 " +
                "on a2=a3 on a1=a3", useSpark);
        testQueryContains(sqlText, "rightouter", methodWatcher, true);
        testQueryContains(sqlText, "leftouter", methodWatcher, true);


        sqlText = format("sparkexplain select * from t1 left join t2 --splice-properties useSpark=%s, useDefaultRowCount=1000000000\n" +
                "full join t3 " +
                "on a2=a3 on a1=a2", useSpark);

        testQueryContains(sqlText, "leftouter", methodWatcher, true);
        queryDoesNotContainString(sqlText, new String[]{"rightouter", "fullouter"}, methodWatcher, true);
    }

    @Test
    public void testCorrelatedSubqueryWithFullJoinInWhereClause() throws Exception {
        String sqlText = format("sparkexplain select b1, a1, b2, a2, c2 from t1 full join t2 --splice-properties useSpark=%s\n on a1=a2 where a1 in (select a3 from t3 where b2=b3)", useSpark);
        /* full join is converted to inner join due to the two predicates a1=a3 and b2=b3 */
        testQueryContains(sqlText, "Inner", methodWatcher, true);
        testQueryContains(sqlText, "LeftSemi", methodWatcher, true);
    }

    @Test
    public void testCorrelatedSSQWithFullJoin() throws Exception {
        String sqlText = format("sparkexplain select (select a5 from t5 where a1=a5) as ssq1, (select a5 from t5 where a2=a5) as ssq2, t1.*, t2.*  from t1 full join t2 --splice-properties useSpark=%s\n on a1=a2", useSpark);
        String expected = "fullouter";
        testQueryContains(sqlText, expected, methodWatcher, true);
    }

    @Test
    public void testFullJoinInSubquery1() throws Exception {
        String sqlText = format("sparkexplain select * from t11 where c1 in (select c1 from t1 full join t2 --splice-properties useSpark=%s\n on a1=a2 where t11.b1=t1.b1)", useSpark);
        testQueryContains(sqlText, "leftouter", methodWatcher, true);
        testQueryContains(sqlText, "subquery", methodWatcher, true);
    }


    @Test
    public void testFullJoinInDerivedTable() throws Exception {
        String sqlText = format("sparkexplain select * from t11, (select * from t1 full join t2 --splice-properties useSpark=%s, joinStrategy=BROADCAST\n " +
                "on a1=a2) dt where t11.a1 = dt.a2", useSpark);
        testQueryContains(sqlText, "broadcast", methodWatcher, true);
    }

    @Test
    public void testFullJoinWithClause() throws Exception {
        String sqlText = format("sparkexplain with dt as select * from t1 full join t2 --splice-properties useSpark=%s\n on a1=a2\n" +
                "select * from t11, dt where t11.a1 = dt.a2", useSpark);
        testQueryContains(sqlText, "outer", methodWatcher, true);
    }

    @Test
    public void testAggregationOnTopOfFullJoin() throws Exception {
        String sqlText = format("sparkexplain with dt as select a2, count(*) as CC from t1 full join t2 --splice-properties useSpark=%s\n on a1=a2 group by a2 \n" +
                "select * from t11, dt where t11.a1 = dt.a2", useSpark);
        testQueryContains(sqlText, "outer", methodWatcher, true);
        testQueryContains(sqlText, Arrays.asList("GroupBy", "count"), methodWatcher, true);
    }

    @Test
    public void testWindowFunctionOnTopOfFullJoin() throws Exception {
        String sqlText = format("sparkexplain select case when a1 <=3 then 'A' else 'B' end as pid, t1.*, t2.*, max(c2) over (partition by case when a1 <=3 then 'A' else 'B' end order by a1, a2 rows between unbounded preceding and current row) as cmax from t1 full join t2 --splice-properties useSpark=%s\n on a1>a2 order by pid, a1, a2", useSpark);
        testQueryContains(sqlText, "fullouter", methodWatcher, true);
        testQueryContains(sqlText, "window", methodWatcher, true);
    }

    @Test
    public void testFullJoinInUnionAll() throws Exception {
        // predicate outside union-all cannot be pushed inside full outer join
        String sqlText = format("sparkexplain select * from (select a1 as X from t1 full join t2 --splice-properties useSpark=%s\n" +
                "on a1=a2\n" +
                "union all\n" +
                "select a5 as X from t5) dt where X in (1,3,5,7)", useSpark);

        testQueryContains(sqlText, "outer", methodWatcher, true);
        testQueryContains(sqlText, "union", methodWatcher, true);
    }

    @Test
    public void testFullJoinWithIndexAndOrderBy() throws Exception {
        // plan should not skip the OrderBy operation
        String sqlText = format("sparkexplain select b4,c4, b5, c5 from t4 --splice-properties index=idx_t4\n " +
                "full join t5 --splice-properties index=idx_t5, useSpark=%s\n on b4=b5 order by b4", useSpark);
        testQueryContains(sqlText, "fullouter", methodWatcher, true);
        testQueryContains(sqlText, "IndexScan", methodWatcher, true);
    }

    @Test
    public void testLeftIsNonCoveringIndex() throws Exception {
        String sqlText = format("sparkexplain select * from t11 --splice-properties index=idx_t11\n " +
                "full join t2 --splice-properties useSpark=%s\n on b1=b2", useSpark);

        testQueryContains(sqlText, "fullouter", methodWatcher, true);
        testQueryContains(sqlText, "IndexLookup", methodWatcher, true);
    }

    @Test
    public void testTPCH_Q3() throws Exception {
        String sqlText = "sparkexplain\n" +
        "select\n" +
        "        l_orderkey,\n" +
        "        sum(l_extendedprice * (1 - l_discount)) as revenue,\n" +
        "        o_orderdate,\n" +
        "        o_shippriority\n" +
        "from\n" +
        "        customer,\n" +
        "        orders,\n" +
        "        lineitem\n" +
        "where\n" +
        "        c_mktsegment = 'BUILDING'\n" +
        "        and c_custkey = o_custkey\n" +
        "        and l_orderkey = o_orderkey\n" +
        "        and o_orderdate < date('1995-03-15')\n" +
        "        and l_shipdate > date('1995-03-15')\n" +
        "group by\n" +
        "        l_orderkey,\n" +
        "        o_orderdate,\n" +
        "        o_shippriority\n" +
        "order by\n" +
        "        revenue desc,\n" +
        "        o_orderdate\n" +
        "{limit 10}";

        // The plan should look something like this:

        // Native Spark Execution Plan
        //----------------------------
        //-> ScrollInsensitive(RS=15,totalCost=83017.199, outputRows=10,outputHeapSize=293.949 KB, partitions=1)
        //  -> Limit(RS=14, totalCost=83016.888,outputRows=10, outputHeapSize=293.949KB, partitions=1, offset=0,fetchFirst=10)
        //    -> NativeSparkDataSet
        //         *Sort [c1#69456 DESC NULLS FIRST, c2#69457 ASC NULLS LAST], true, 0
        //         +- Exchange rangepartitioning(c1#69456 DESC NULLS FIRST, c2#69457 ASC NULLS LAST, 200)
        //            +- *HashAggregate(keys=[c0#69346L, c1#69347, c2#69348], functions=[sum(c4#69350)], output=[c0#69346L, c1#69456, c2#69457, c3#69458])
        //               +- Exchange hashpartitioning(c0#69346L, c1#69347, c2#69348, 200)
        //                  +- *HashAggregate(keys=[c0#69346L, c1#69347, c2#69348], functions=[partial_sum(c4#69350)], output=[c0#69346L, c1#69347, c2#69348, sum#69484])
        //                     +- *Project [c0#69346L, c1#69347, c2#69348, c4#69350]
        //                        +- Scan ExistingRDD[c0#69346L,c1#69347,c2#69348,c3#69349,c4#69350,c5#69351]
        //                           -> ProjectRestrict(RS=10,totalCost=5721.784, outputRows=514430,outputHeapSize=92.411 MB, partitions=1)
        //                             -> MergeJoin(RS=8, totalCost=5721.784,outputRows=514430, outputHeapSize=92.411MB, partitions=1,preds=[(L_ORDERKEY[8:7] =O_ORDERKEY[8:1])])
        //                               -> TableScan[LINEITEM(1584)](RS=6,totalCost=11286.284,scannedRows=6001215, outputRows=3249224,outputHeapSize=92.411 MB, partitions=1,preds=[(L_SHIPDATE[6:4] > 1995-03-15)])
        //                               -> NativeSparkDataSet
        //                                 *Project [c0#69252L, c1#69253, c2#69254, c3#69255, c0#69272 AS c4#69330, c1#69273 AS c5#69331]
        //                                 +- *BroadcastHashJoin [c1#69253], [c0#69272], Inner, BuildRight
        //                                    :- *Filter isnotnull(c1#69253)
        //                                    :  +- Scan ExistingRDD[c0#69252L,c1#69253,c2#69254,c3#69255]
        //                                          -> TableScan[ORDERS(1632)](RS=0,totalCost=3004, scannedRows=1500000,outputRows=731791, outputHeapSize=31.017MB, partitions=1,preds=[(O_ORDERDATE[0:3] < 1995-03-15)])
        //                                    +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)))
        //                                       +- *Filter isnotnull(c0#69272)
        //                                          +- Scan ExistingRDD[c0#69272,c1#69273]
        //                                             -> TableScan[CUSTOMER(1648)](RS=2,totalCost=383.5, scannedRows=150000,outputRows=30142, outputHeapSize=10.946MB, partitions=1,preds=[(C_MKTSEGMENT[2:2] = BUILDING)])

        testQueryContains(sqlText, Arrays.asList("ExistingRDD", "Limit"), methodWatcher, true);
    }

    @Test
    public void testTPCH_Q5() throws Exception {
        String sqlText = "sparkexplain select\n" +
        "        n_name,\n" +
        "        sum(l_extendedprice * (1 - l_discount)) as revenue\n" +
        "from\n" +
        "        customer,\n" +
        "        orders,\n" +
        "        lineitem,\n" +
        "        supplier,\n" +
        "        nation,\n" +
        "        region\n" +
        "where\n" +
        "        c_custkey = o_custkey\n" +
        "        and l_orderkey = o_orderkey\n" +
        "        and l_suppkey = s_suppkey\n" +
        "        and c_nationkey = s_nationkey\n" +
        "        and s_nationkey = n_nationkey\n" +
        "        and n_regionkey = r_regionkey\n" +
        "        and r_name = 'ASIA'\n" +
        "        and o_orderdate >= date('1994-01-01')\n" +
        "        and o_orderdate < date({fn TIMESTAMPADD(SQL_TSI_YEAR, 1, cast('1994-01-01 00:00:00' as timestamp))})\n" +
        "group by\n" +
        "        n_name\n" +
        "order by\n" +
        "        revenue desc";

        // The plan should look something like this:

        // Native Spark Execution Plan
        //----------------------------
        //-> NativeSparkDataSet
        //     *Sort [c1#3178 DESC NULLS FIRST], true, 0
        //     +- Exchange rangepartitioning(c1#3178 DESC NULLS FIRST, 200)
        //        +- *HashAggregate(keys=[c0#3125], functions=[sum(c2#3127)], output=[c0#3125, c1#3178])
        //           +- Exchange hashpartitioning(c0#3125, 200)
        //              +- *HashAggregate(keys=[c0#3125], functions=[partial_sum(c2#3127)], output=[c0#3125, sum#3194])
        //                 +- *Project [c10#2800 AS c0#3125, CheckOverflow((cast(c2#2512 as decimal(36,2)) * cast(CheckOverflow((1.00 - cast(c3#2513 as decimal(16,2))), DecimalType(16,2)) as decimal(36,2))), DecimalType(38,4)) AS c2#3127]
        //                    +- *SortMergeJoin [c8#2678, c5#2583], [c1#3018, c0#3017], Inner
        //                       :- *Sort [c8#2678 ASC NULLS FIRST, c5#2583 ASC NULLS FIRST], false, 0
        //                       :  +- Exchange hashpartitioning(c8#2678, c5#2583, 200)
        //                       :     +- *Project [c2#2512, c3#2513, c5#2583, c8#2678, c10#2800]
        //                       :        +- *BroadcastHashJoin [c11#2801], [c0#2817], Inner, BuildRight
        //                       :           :- *Project [c2#2512, c3#2513, c5#2583, c8#2678, c1#2693 AS c10#2800, c2#2694 AS c11#2801]
        //                       :           :  +- *BroadcastHashJoin [c8#2678], [c0#2692], Inner, BuildRight
        //                       :           :     :- *Project [c2#2512, c3#2513, c5#2583, c1#2596 AS c8#2678]
        //                       :           :     :  +- *BroadcastHashJoin [c1#2511], [c0#2595], Inner, BuildRight
        //                       :           :     :     :- *Project [c1#2511, c2#2512, c3#2513, c1#2532 AS c5#2583]
        //                       :           :     :     :  +- *SortMergeJoin [c0#2510L], [c0#2531L], Inner
        //                       :           :     :     :     :- *Sort [c0#2510L ASC NULLS FIRST], false, 0
        //                       :           :     :     :     :  +- Exchange hashpartitioning(c0#2510L, 200)
        //                       :           :     :     :     :     +- *Filter (isnotnull(c0#2510L) && isnotnull(c1#2511))
        //                       :           :     :     :     :        +- Scan ExistingRDD[c0#2510L,c1#2511,c2#2512,c3#2513]
        //                                                                 -> IndexScan[L_PART_IDX(1601)](RS=0,totalCost=8645.75, scannedRows=6001215,outputRows=6001215,outputHeapSize=125.911 MB,partitions=35, baseTable=LINEITEM(1584))
        //                       :           :     :     :     +- *Sort [c0#2531L ASC NULLS FIRST], false, 0
        //                       :           :     :     :        +- Exchange hashpartitioning(c0#2531L, 200)
        //                       :           :     :     :           +- *Project [c0#2531L, c1#2532]
        //                       :           :     :     :              +- *Filter (isnotnull(c0#2531L) && isnotnull(c1#2532))
        //                       :           :     :     :                 +- Scan ExistingRDD[c0#2531L,c1#2532,c2#2533]
        //                                                                    -> TableScan[ORDERS(1632)](RS=2,totalCost=3004, scannedRows=1500000,outputRows=412750, outputHeapSize=48.541MB, partitions=35,preds=[(O_ORDERDATE[2:3] <date(TIMESTAMPADD(1994-01-01 00:00:00.0,8, 1) )),(O_ORDERDATE[2:3] >=1994-01-01)])
        //                       :           :     :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)))
        //                       :           :     :        +- *Filter (isnotnull(c0#2595) && isnotnull(c1#2596))
        //                       :           :     :           +- Scan ExistingRDD[c0#2595,c1#2596]
        //                                                        -> TableScan[SUPPLIER(1680)](RS=6,totalCost=27.4, scannedRows=10000,outputRows=10000, outputHeapSize=48.906MB, partitions=35)
        //                       :           :     +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, false] as bigint)))
        //                       :           :        +- *Filter (isnotnull(c0#2692) && isnotnull(c2#2694))
        //                       :           :           +- Scan ExistingRDD[c0#2692,c1#2693,c2#2694]
        //                                                  -> TableScan[NATION(1728)](RS=10,totalCost=4.047, scannedRows=25,outputRows=25, outputHeapSize=48.907 MB,partitions=35)
        //                       :           +- BroadcastExchange HashedRelationBroadcastMode(List(cast(input[0, int, true] as bigint)))
        //                       :              +- *Project [c0#2817]
        //                       :                 +- *Filter isnotnull(c0#2817)
        //                       :                    +- Scan ExistingRDD[c0#2817,c1#2818]
        //                                               -> TableScan[REGION(1712)](RS=14,totalCost=4.009, scannedRows=5,outputRows=1, outputHeapSize=9.782 MB,partitions=35, preds=[(R_NAME[14:2] =ASIA)])
        //                       +- *Sort [c1#3018 ASC NULLS FIRST, c0#3017 ASC NULLS FIRST], false, 0
        //                          +- Exchange hashpartitioning(c1#3018, c0#3017, 200)
        //                             +- *Filter (isnotnull(c1#3018) && isnotnull(c0#3017))
        //                                +- Scan ExistingRDD[c0#3017,c1#3018]
        //                                   -> TableScan[CUSTOMER(1648)](RS=18,totalCost=383.5, scannedRows=150000,outputRows=150000, outputHeapSize=20.924MB, partitions=35)
        testQueryContains(sqlText, Arrays.asList("partial_sum", "GroupBy"), methodWatcher, true);
    }

    @Test
    public void testCrossJoinCartesianProduct() throws Exception {
        if (useSpark.equalsIgnoreCase("false")) return; // cross join only has Spark implementation
        String sqlText = "sparkexplain select count(*) from --splice-properties joinOrder=fixed\n" +
                "        big --splice-properties useSpark=true, joinStrategy=cross\n" +
                "        inner join big on 1=1";
        testQueryContains(sqlText, "CartesianProduct", methodWatcher, true);
    }

    @Test
    public void testCrossJoinBroadcastRight() throws Exception {
        if (useSpark.equalsIgnoreCase("false")) return; // cross join only has Spark implementation
        String sqlText = "sparkexplain select count(*) from --splice-properties joinOrder=fixed\n" +
                "        t1 --splice-properties useSpark=true, joinStrategy=cross\n" +
                "        inner join big on 1=1";
        testQueryContains(sqlText, "BroadcastNestedLoopJoin BuildRight, Cross", methodWatcher, true);
    }

    @Test
    public void testCrossJoinNeverBroadcastLeft() throws Exception {
        /* DB-9579
         * In case of a cross join, we only check if the right side can be broadcast and never broadcast the left
         * side. Reason is that when the plan reaches Spark and it doesn't have a sort node, it might be the case
         * that there are sort keys but the sort node is optimized away because the left side row order satisfies
         * the sort keys. If we broadcast the left side, resulting row order might be different.
         */
        if (useSpark.equalsIgnoreCase("false")) return; // cross join only has Spark implementation
        String sqlText = "sparkexplain select count(*) from --splice-properties joinOrder=fixed\n" +
                "        big --splice-properties useSpark=true, joinStrategy=cross\n" +
                "        inner join t1 on 1=1";
        testQueryContains(sqlText, "CartesianProduct", methodWatcher, true);
    }

    @Test
    public void testCrossJoinBroadcastRightNoHint() throws Exception {
        if (useSpark.equalsIgnoreCase("false")) return; // cross join only has Spark implementation
        String sqlText = "explain select count(*) from\n" +
                "        t1 --splice-properties useSpark=true\n" +
                "        inner join big on 1=1";
        // BIG table should be on the left side (bigger row index)
        String[] expectedList = {"CrossJoin", "TableScan[T1(", "TableScan[BIG("};
        rowContainsQuery(new int[]{6, 7, 9}, sqlText, methodWatcher, expectedList);

        sqlText = "sparkexplain select count(*) from\n" +
                "        t1 --splice-properties useSpark=true\n" +
                "        inner join big on 1=1";
        // expecting broadcast on the right side and BIG table is on the left side
        String[] expectedList2 = {"BroadcastNestedLoopJoin BuildRight, Cross", ":- Scan ExistingRDD[]", "-> TableScan[BIG("};
        rowContainsQuery(new int[]{5, 6, 7}, sqlText, methodWatcher, expectedList2);
    }

    @Test
    public void testCrossJoinForceBroadcastRight() throws Exception {
        if (useSpark.equalsIgnoreCase("false")) return; // cross join only has Spark implementation
        // use the same query as testCrossJoinCartesianProduct, not broadcasting right side based on cost,
        // but we force it by giving broadcastCrossRight=true hint
        String sqlText = "sparkexplain select count(*) from --splice-properties joinOrder=fixed\n" +
                "        big --splice-properties useSpark=true, joinStrategy=cross, broadcastCrossRight=true\n" +
                "        inner join big on 1=1";
        testQueryContains(sqlText, "BroadcastNestedLoopJoin BuildRight, Cross", methodWatcher, true);
    }

    @Test
    public void testCrossJoinForceCartesianProduct() throws Exception {
        if (useSpark.equalsIgnoreCase("false")) return; // cross join only has Spark implementation
        // use the same query as testCrossJoinBroadcastRight, broadcasting right side based on cost,
        // but we force not to by giving broadcastCrossRight=false
        String sqlText = "sparkexplain select count(*) from --splice-properties joinOrder=fixed\n" +
                "        t1 --splice-properties useSpark=true, joinStrategy=cross, broadcastCrossRight=false\n" +
                "        inner join big on 1=1";
        testQueryContains(sqlText, "CartesianProduct", methodWatcher, true);
    }
}
