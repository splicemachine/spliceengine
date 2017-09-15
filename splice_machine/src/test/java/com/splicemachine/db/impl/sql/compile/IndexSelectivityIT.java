/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
import java.sql.ResultSet;
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
    }

    @Test
    public void testCoveringIndexScan() throws Exception {
        rowContainsQuery(3,"explain select c1 from ts_low_cardinality where c1 = 1","IndexScan[TS_LOW_CARDINALITY_IX_1",methodWatcher);
        rowContainsQuery(3,"explain select c1,c2 from ts_low_cardinality where c1 = 1","IndexScan[TS_LOW_CARDINALITY_IX_3",methodWatcher);
        rowContainsQuery(3,"explain select c2 from ts_low_cardinality where c2 = '1'","IndexScan[TS_LOW_CARDINALITY_IX_2",methodWatcher);
        rowContainsQuery(6,"explain select count(*) from ts_low_cardinality where c2 = '1'","IndexScan[TS_LOW_CARDINALITY_IX_2",methodWatcher);
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

}
