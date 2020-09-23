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
import com.splicemachine.test_tools.TableCreator;
import org.junit.*;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

/**
 *
 *
 *
 */
public class IndexSelectivityIT extends SpliceUnitTest {
    public static final String CLASS_NAME = IndexSelectivityIT.class.getSimpleName().toUpperCase();
    protected static SpliceWatcher spliceClassWatcher = new SpliceWatcher(CLASS_NAME);
    protected static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(CLASS_NAME);

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher);
    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(CLASS_NAME);

    @BeforeClass
    public static void createDataSet() throws Exception {
        Connection conn = spliceClassWatcher.getOrCreateConnection();
        new TableCreator(conn)
                .withCreate("create table ts_low_cardinality (c1 int, c2 varchar(56), c3 timestamp, c4 boolean)")
                .withInsert("insert into ts_low_cardinality values(?,?,?,?)")
                .withRows(rows(
                        row(1, "1", "1960-01-01 23:03:20", false),
                        row(2, "2", "1980-01-01 23:03:20", false),
                        row(3, "3", "1985-01-01 23:03:20", false),
                        row(4, "4", "1990-01-01 23:03:20", false),
                        row(5, "5", "1995-01-01 23:03:20", false),
                        row(null, null, null, null),
                        row(null, null, null, null),
                        row(null, null, null, null)))
                .withIndex("create index ts_low_cardinality_ix_1 on ts_low_cardinality(c1)")
                .withIndex("create index ts_low_cardinality_ix_2 on ts_low_cardinality(c2)")
                .withIndex("create index ts_low_cardinality_ix_3 on ts_low_cardinality(c1,c2)")
                .withIndex("create index ts_low_cardinality_ix_4 on ts_low_cardinality(c1,c2,c3)")
                .withIndex("create index ts_low_cardinality_ix_5 on ts_low_cardinality(c1,c2,c3,c4)")
                .create();
        for (int i = 0; i < 10; i++) {
            spliceClassWatcher.executeUpdate("insert into ts_low_cardinality select * from ts_low_cardinality");
        }

        new TableCreator(conn)
                .withCreate("create table ts_high_cardinality (c1 int, c2 varchar(56), c3 timestamp, c4 boolean)")
                .withIndex("create index ts_high_cardinality_ix_1 on ts_high_cardinality(c1)")
                .withIndex("create index ts_high_cardinality_ix_2 on ts_high_cardinality(c2)")
                .withIndex("create index ts_high_cardinality_ix_3 on ts_high_cardinality(c1,c2)")
                .withIndex("create index ts_high_cardinality_ix_4 on ts_high_cardinality(c1,c2,c3)")
                .create();

        PreparedStatement insert = spliceClassWatcher.prepareStatement("insert into ts_high_cardinality values (?,?,?,?)");

        long time = System.currentTimeMillis();
        for (int i = 0; i < 10000; i++) {
            insert.setInt(1,i);
            insert.setString(2, "" + i);
            insert.setTimestamp(3,new Timestamp(time-i));
            insert.setBoolean(4,false);
            insert.addBatch();
            if (1%100==0)
                insert.executeBatch();
        }
        insert.executeBatch();

        new TableCreator(conn)
                .withCreate("create table narrow_table(i int, j int)")
                .withInsert("insert into narrow_table values(?,?)")
                .withRows(rows(
                        row(1, 2),
                        row(3, 4)))
                .withIndex("create index narrow_table_idx on narrow_table(i)")
                .create();

        PreparedStatement doubleSize = spliceClassWatcher.prepareStatement("insert into narrow_table select * from narrow_table");

        for (int i = 0; i < 6; i++) {
            doubleSize.execute();
        }
        conn.commit();
        new TableCreator(conn)
                .withCreate("create table wide_table(i int, j int, k int, l int, m int, n int)")
                .withInsert("insert into wide_table values(?,?,?,?,?,?)")
                .withRows(rows(
                        row(1, 2, 3, 4, 5, 6),
                        row(3, 4, 5, 6, 7, 8)))
                .withIndex("create index wide_table_idx on wide_table(i)")
                .create();

        doubleSize = spliceClassWatcher.prepareStatement("insert into wide_table select * from wide_table");

        for (int i = 0; i < 6; i++) {
            doubleSize.execute();
        }
        conn.commit();
        new TableCreator(conn)
                .withCreate("create table wide_table_pk(i int, j int, k int, l int, m int, n int, primary key (j,k,l,m,n))")
                .withInsert("insert into wide_table_pk values(?,?,?,?,?,?)")
                .withRows(rows(
                        row(1, 1, 3, 4, 5, 6),
                        row(3, 2, 5, 6, 7, 8)))
                .withIndex("create index wide_table_pk_idx on wide_table_pk(i)")
                .create();


        for (int i = 0; i < 6; i++) {
            spliceClassWatcher.execute("insert into wide_table_pk select i,j+(select count(*) from wide_table_pk),k,l,m,n from wide_table_pk");
        }
        conn.commit();

        conn.createStatement().executeQuery(format(
                "call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS('%s',false)",
                spliceSchemaWatcher));
        conn.commit();

        new TableCreator(conn)
                .withCreate("create table t1 (c1 int, c2 int, c3 int, c4 int, c5 int, c6 int, c7 int, c8 int, c9 int, c10 int, c11 int, c12 int, primary key (c1))")
                .withInsert("insert into t1 values(?,?,?,?,?,?,?,?,?,?,?,?)")
                .withRows(rows(
                        row(0,0,0,0,0,0,0,0,0,0,0,0),
                        row(1,1,1,1,1,1,1,1,1,1,1,1),
                        row(2,2,2,2,2,2,2,2,2,2,2,2),
                        row(3,3,3,3,3,3,3,3,3,3,3,3),
                        row(4,4,4,4,4,4,4,4,4,4,4,4),
                        row(5,5,5,5,5,5,5,5,5,5,5,5),
                        row(6,6,6,6,6,6,6,6,6,6,6,6),
                        row(7,7,7,7,7,7,7,7,7,7,7,7),
                        row(8,8,8,8,8,8,8,8,8,8,8,8),
                        row(9,9,9,9,9,9,9,9,9,9,9,9)))
                .withIndex("create index t1_idx on t1(c3,c4,c5)")
                .create();

        // we purposely leave t1 out from stats collection, as the test case is to test the plan selection without stats.
        int factor = 10;
        for (int i = 1; i <= 12; i++) {
            spliceClassWatcher.executeUpdate(format("insert into t1 select c1+%d, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12 from t1", factor));
            factor = factor * 2;
        }
        conn.commit();

        new TableCreator(conn)
                .withCreate("create table splice2250 (a1 int, b1 date, c1 int, d1 time, e1 timestamp, f1 real, g1 double)")
                .withInsert("insert into splice2250 values(?,?,?,?,?,?,?)")
                .withRows(rows(
                        row(1, "2018-01-01", 1,"15:01:01","2018-01-01 17:12:30",1.0,1.1),
                        row(2, "2018-01-02", 2,"15:02:02","2018-01-02 17:12:30",2.1,2.0),
                        row(3, "2018-01-03", 3,"15:03:03","2018-01-03 17:12:30",3.3,3.3),
                        row(4, "2018-01-04", 4,"15:04:04","2018-01-04 17:12:30",4.4,4.4),
                        row(5, "2018-01-05", 5,"15:05:05","2018-01-05 17:12:30",5.5,5.5),
                        row(6, "2018-01-06", 6,"15:06:06","2018-01-06 17:12:30",6.6,6.6),
                        row(7, "2018-01-07", 7,"15:07:07","2018-01-07 17:12:30",7.7,7.7),
                        row(8, "2018-01-08", 8,"15:08:08","2018-01-08 17:12:30",8.8,8.8),
                        row(9, "2018-01-09", 9,"15:09:09","2018-01-09 00:00:00",9.9,9.9),
                        row(10, "2018-01-10", 10,"15:10:10","2018-01-10 17:12:30",10.1,10.1)))
                .create();

        spliceClassWatcher.executeUpdate("insert into splice2250 select a1+10,b1,c1,d1,e1,f1,g1 from splice2250");
        spliceClassWatcher.executeUpdate("insert into splice2250 select a1+20,b1,c1,d1,e1,f1,g1 from splice2250");
        spliceClassWatcher.executeUpdate("insert into splice2250 select a1+40,b1,c1,d1,e1,f1,g1 from splice2250");
        spliceClassWatcher.executeUpdate("insert into splice2250 select a1+80,b1,c1,d1,e1,f1,g1 from splice2250");
        spliceClassWatcher.execute("analyze table splice2250");
        spliceClassWatcher.execute("create index ix_t1_1 on splice2250 (b1, c1) ");
        spliceClassWatcher.execute("create index ix_t1_2 on splice2250 (d1) ");
        spliceClassWatcher.execute("create index ix_t1_3 on splice2250 (e1) ");
        spliceClassWatcher.execute("create index ix_t1_4 on splice2250 (f1) ");
        spliceClassWatcher.execute("create index ix_t1_5 on splice2250 (g1) ");

    }

    @Test
    public void testCoveringIndexScan() throws Exception {
        rowContainsQuery(3,"explain select c1 from ts_low_cardinality where c1 = 1","IndexScan[TS_LOW_CARDINALITY_IX_1",methodWatcher);
        rowContainsQuery(3,"explain select c1,c2 from ts_low_cardinality where c1 = 1","IndexScan[TS_LOW_CARDINALITY_IX_3",methodWatcher);
        rowContainsQuery(3,"explain select c2 from ts_low_cardinality where c2 = '1'","IndexScan[TS_LOW_CARDINALITY_IX_2",methodWatcher);
        rowContainsQuery(6,"explain select count(*) from ts_low_cardinality where c2 = '1'","IndexScan[TS_LOW_CARDINALITY_IX_2",methodWatcher);
    }

    @Test
    public void testCoveringIndexOverBaseTableScanWithoutStats () throws Exception {
        //covering index should be picked
        rowContainsQuery(3, "explain select c3, c4, c5 from t1 where c4=10", "IndexScan[T1_IDX", methodWatcher);
        //if not covering index, base table scan should be picked
        rowContainsQuery(3, "explain select c3, c4, c5, c6 from t1 where c4=10", "TableScan[T1", methodWatcher);

    }

    @Test @Ignore
    public void testSingleRowIndexLookup() throws Exception {
        rowContainsQuery(4,"explain select * from ts_high_cardinality where c1 = 1","IndexScan[TS_HIGH_CARDINALITY_IX",methodWatcher);
    }

    @Test
    // Partially obsoleted by testRangeIndexLookup1 and testRangeIndexLookup2.
    public void testRangeIndexLookup() throws Exception {
        // 200/10000
        rowContainsQuery(3,"explain select * from ts_high_cardinality where c1 > 1 and c1 < 200","TableScan[TS_HIGH_CARDINALITY",methodWatcher);
        // 1000/10000
        rowContainsQuery(3,"explain select * from ts_high_cardinality where c1 > 1 and c1 < 1000","TableScan[TS_HIGH_CARDINALITY",methodWatcher);
        // 2000/10000
        rowContainsQuery(3,"explain select * from ts_high_cardinality where c1 > 1 and c1 < 2000","TableScan[TS_HIGH_CARDINALITY",methodWatcher);
        // 5000/10000
        rowContainsQuery(3,"explain select * from ts_high_cardinality where c1 > 1 and c1 < 5000","TableScan[TS_HIGH_CARDINALITY",methodWatcher);
    }
    
    @Test
    public void testRangeIndexLookup1() throws Exception {
    	// Instead of running unhinted explain and asserting that a TableScan or IndexScan
    	// is selected (which is what we used to do here), now we hint with a specific index
    	// and assert the more specific outcome of correct outputRows for both
    	// the IndexScan and IndexLookup.
    	
    	String index = "TS_HIGH_CARDINALITY_IX_1";
    	String query = "explain select * from ts_high_cardinality --SPLICE-PROPERTIES index=%s \n where c1 > 1 and c1 < %d";

        double variation = 10000.0d*.02;

    	// 10/10000
        rowContainsQuery(new int[]{3, 4},
        	format(query, index, 10),
            methodWatcher,
            "IndexLookup",
            "IndexScan[TS_HIGH_CARDINALITY_IX_1");

        rowContainsCount(new int[]{3, 4},
                format(query, index, 10),
                methodWatcher,
                new double[]{8.0d,8.0d},
                new double[]{variation,variation});


        // 100/10000
        rowContainsQuery(new int[]{3, 4},
        	format(query, index, 100),
            methodWatcher,
            "IndexLookup",
            "IndexScan[TS_HIGH_CARDINALITY_IX_1");

        rowContainsCount(new int[]{3, 4},
                format(query, index, 100),
                methodWatcher,
                new double[]{98.0d,98.0d},
                new double[]{variation,variation});


        // 200/10000
        rowContainsQuery(new int[]{3, 4},
        	format(query, index, 200),
            methodWatcher,
            "IndexLookup",
            "IndexScan[TS_HIGH_CARDINALITY_IX_1");

        rowContainsCount(new int[]{3, 4},
                format(query, index, 200),
                methodWatcher,
                new double[]{198.0d,198.0d},
                new double[]{variation,variation});

        // 1000/10000
        rowContainsQuery(new int[]{3, 4},
        	format(query, index, 1000),
            methodWatcher,
            "IndexLookup",
            "IndexScan[TS_HIGH_CARDINALITY_IX_1");

        rowContainsCount(new int[]{3, 4},
                format(query, index, 1000),
                methodWatcher,
                new double[]{998.0d,998.0d},
                new double[]{variation,variation});

        // 2000/10000
        rowContainsQuery(new int[]{3, 4},
        	format(query, index, 1999),
            methodWatcher,
            "IndexLookup",
            "IndexScan[TS_HIGH_CARDINALITY_IX_1");

        rowContainsCount(new int[]{3, 4},
                format(query, index, 1999),
                methodWatcher,
                new double[]{1998.0d,1998},
                new double[]{variation,variation});


        // 5000/10000
        rowContainsQuery(new int[]{3, 4},
        	format(query, index, 5000),
            methodWatcher,
            "IndexLookup",
            "IndexScan[TS_HIGH_CARDINALITY_IX_1");

        rowContainsCount(new int[]{3, 4},
                format(query, index, 5000),
                methodWatcher,
                new double[]{4998.0d,4998.0d},
                new double[]{variation,variation});

    }

    @Test
    public void testRangeIndexLookup2() throws Exception {
        // Similar to testRangeIndexLookup1 except use index 2 (TS_HIGH_CARDINALITY_IX_2)
    	// even though we still filter by C1. This will introduce ProjectRestrict
    	// into the plan, but it should still have the correct outputRows.
    	// DB-3872 caused the ProjectRestrict outputRows to be incorrect.

    	String index2 = "TS_HIGH_CARDINALITY_IX_2";
    	String query = "explain select * from ts_high_cardinality --SPLICE-PROPERTIES index=%s \n where c1 > 1 and c1 < %d";
        double variation = 10000.0d*.02;

        // 10/10000
        rowContainsQuery(new int[]{3},
        	format(query, index2, 10),
            methodWatcher,
            "ProjectRestrict");

        rowContainsCount(new int[]{3},
                format(query, index2, 10),
                methodWatcher,
                new double[]{8.0d},
                new double[]{variation});

        // 100/10000
        rowContainsQuery(new int[]{3},
        	format(query, index2, 100),
            methodWatcher,
            "ProjectRestrict");
        rowContainsCount(new int[]{3},
                format(query, index2, 100),
                methodWatcher,
                new double[]{98.0d},
                new double[]{variation});


        // 200/10000
        rowContainsQuery(new int[]{3},
        	format(query, index2, 200),
            methodWatcher,
            "ProjectRestrict");

        rowContainsCount(new int[]{3},
                format(query, index2, 200),
                methodWatcher,
                new double[]{198.0d},
                new double[]{variation});

        // 1000/10000
        rowContainsQuery(new int[]{3},
        	format(query, index2, 1000),
            methodWatcher,
            "ProjectRestrict");

        rowContainsCount(new int[]{3},
                format(query, index2, 1000),
                methodWatcher,
                new double[]{998.0d},
                new double[]{variation});

        // 2000/10000
        rowContainsQuery(new int[]{3},
        	format(query, index2, 2000),
            methodWatcher, "ProjectRestrict");

        rowContainsCount(new int[]{3},
                format(query, index2, 2000),
                methodWatcher,
                new double[]{1998.0d},
                new double[]{variation});


        // 5000/10000
        rowContainsQuery(new int[]{3},
        	format(query, index2, 5000),
            methodWatcher,
            "ProjectRestrict");

        rowContainsCount(new int[]{3},
                format(query, index2, 5000),
                methodWatcher,
                new double[]{4998},
                new double[]{variation});

    }

    @Test
    @Ignore("Splice-1097")
    public void testCountChoosesNarrowTable() throws Exception {
        rowContainsQuery(6,"explain select count(*) from narrow_table","TableScan[NARROW_TABLE",methodWatcher);
    }

    @Test
    public void testFilteredCountChoosesNarrowTableIndex() throws Exception {
        rowContainsQuery(6,"explain select count(*) from narrow_table where i = 1","IndexScan[NARROW_TABLE_IDX",methodWatcher);
    }

    @Test
    public void testCountChoosesWideTableIndex() throws Exception {
        rowContainsQuery(6,"explain select count(*) from wide_table","IndexScan[WIDE_TABLE_IDX",methodWatcher);
    }

    @Test
    @Ignore("Splice-1097")
    public void testCountChoosesWideTablePK() throws Exception {
        rowContainsQuery(6,"explain select count(*) from wide_table_pk","TableScan[WIDE_TABLE_PK",methodWatcher);
    }

    // Possible future tests:
    // testNonCoveringIndexScan
    // testMostSelectiveIndexChosen
    // test1PercentRangeScan
    // test20PercentRangeScan

    @Test
    public void testImplicitCastStringToDate() throws Exception {
        thirdRowContainsQuery("explain select b1,c1 from splice2250 where b1='2018-01-03'","scannedRows=16,outputRows=16",methodWatcher);
    }

    @Test
    public void testImplicitCastStringToTime() throws Exception {
        thirdRowContainsQuery("explain select d1 from splice2250 where d1='15:03:03'","scannedRows=16,outputRows=16",methodWatcher);
    }

    @Test
    public void testImplicitCastStringToTimeStamp() throws Exception {
        thirdRowContainsQuery("explain select e1 from splice2250 where e1='2018-01-03 17:12:30'","scannedRows=16,outputRows=16",methodWatcher);
    }

    @Test
    public void testCastDateToTimeStamp() throws Exception {
        thirdRowContainsQuery("explain select e1 from splice2250 where e1=date('2018-01-09')","scannedRows=16,outputRows=16",methodWatcher);
    }

    @Test
    public void testImplicitCastIntegerToReal() throws Exception {
        thirdRowContainsQuery("explain select f1 from splice2250 where f1=1","scannedRows=16,outputRows=16",methodWatcher);
    }

    @Test
    public void testImplicitCastIntegerToDouble() throws Exception {
        thirdRowContainsQuery("explain select g1 from splice2250 where g1=2","scannedRows=16,outputRows=16",methodWatcher);
    }

    @Test
    public void testCompareTimeStampToDate() throws Exception {
        thirdRowContainsQuery("explain select b1 from splice2250 where b1=timestamp('2018-01-01 00:00:00')","scannedRows=16,outputRows=16",methodWatcher);
    }

}
