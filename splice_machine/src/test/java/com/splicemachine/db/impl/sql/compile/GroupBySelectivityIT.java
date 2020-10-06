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
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Timestamp;

import static com.splicemachine.test_tools.Rows.row;
import static com.splicemachine.test_tools.Rows.rows;

public class GroupBySelectivityIT extends SpliceUnitTest {
    public static final String CLASS_NAME = GroupBySelectivityIT.class.getSimpleName().toUpperCase();
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
                .withIndex("create index ts_low_cardinality_expr_ix_1 on ts_low_cardinality(upper(c2))")
                .withIndex("create index ts_low_cardinality_expr_ix_2 on ts_low_cardinality(mod(c1,3), upper(c2), timestampadd(SQL_TSI_DAY, 2, c3))")
                .create();
        for (int i = 0; i < 10; i++) {
            spliceClassWatcher.executeUpdate("insert into ts_low_cardinality select * from ts_low_cardinality");
        }
        new TableCreator(conn)
                .withCreate("create table ts_high_cardinality (c1 int, c2 varchar(56), c3 timestamp, c4 boolean)").create();

        PreparedStatement insert = spliceClassWatcher.prepareStatement("insert into ts_high_cardinality values (?,?,?,?)");

        long time = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            insert.setInt(1,i);
            insert.setString(2, "" + i);
            insert.setTimestamp(3,new Timestamp(time-i));
            insert.setBoolean(4,false);
            insert.addBatch();
            if (i%100==0)
                insert.executeBatch();
        }
        insert.executeBatch();
        conn.commit();
        conn.createStatement().executeQuery(format(
                "call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS('%s',false)",
                spliceSchemaWatcher));
        conn.commit();

    }

    @Test
    public void testGroupByCardinality() throws Exception {
        secondRowContainsQuery("explain select count(*), c1 from ts_low_cardinality group by c1", "outputRows=5", methodWatcher);
        secondRowContainsQuery("explain select count(*), c2 from ts_low_cardinality group by c2", "outputRows=5", methodWatcher);
        secondRowContainsQuery("explain select count(*), c3 from ts_low_cardinality group by c3", "outputRows=5", methodWatcher);
        secondRowContainsQuery("explain select count(*), c4 from ts_low_cardinality group by c4", "outputRows=1", methodWatcher);

        // group by index expression
        // TableScan and IndexScan scan the same number of rows, but there is no need to evaluate upper(c2) in IndexScan
        rowContainsQuery(new int[]{2,6}, "explain select count(*), upper(c2) from ts_low_cardinality group by upper(c2)", methodWatcher,
                "outputRows=5", "IndexScan[TS_LOW_CARDINALITY_EXPR_IX_1");
    }

    /* Group-by cardinality estimation for multiple predicates doesn't feel right. See comments for method
     * computeCardinality() in TempGroupedAggregateCostController.java.
     */
    @Test
    public void testGroupByCardinalityMultiplication() throws Exception {
        secondRowContainsQuery("explain select count(*), c1,c2 from ts_low_cardinality group by c1,c2", "outputRows=10", methodWatcher);
        secondRowContainsQuery("explain select count(*), c1,c3 from ts_low_cardinality group by c1,c3", "outputRows=10", methodWatcher);
        secondRowContainsQuery("explain select count(*), c1,c4 from ts_low_cardinality group by c1,c4", "outputRows=2", methodWatcher);
        secondRowContainsQuery("explain select count(*), c4,c2 from ts_low_cardinality group by c4,c2", "outputRows=2", methodWatcher);

        rowContainsQuery(new int[]{2,6}, "explain select count(*), mod(c1,3), upper(c2) from ts_low_cardinality group by mod(c1,3), upper(c2)", methodWatcher,
                "outputRows=6", "IndexScan[TS_LOW_CARDINALITY_EXPR_IX_2");   // 16 rows
        rowContainsQuery(new int[]{2,6}, "explain select count(*), mod(c1,3), timestampadd(SQL_TSI_DAY, 2, c3) from ts_low_cardinality group by mod(c1,3), timestampadd(SQL_TSI_DAY, 2, c3)", methodWatcher,
                "outputRows=6", "IndexScan[TS_LOW_CARDINALITY_EXPR_IX_2");   // 16 rows
        rowContainsQuery(new int[]{2,6}, "explain select count(*), upper(c2), timestampadd(SQL_TSI_DAY, 2, c3) from ts_low_cardinality group by upper(c2), timestampadd(SQL_TSI_DAY, 2, c3)", methodWatcher,
                "outputRows=10", "IndexScan[TS_LOW_CARDINALITY_EXPR_IX_2");  // 26 rows
    }

    /**
     *
     * Should Selectivity of releation effect the distribution of group by (probably when they are large, not when they are small?)
     *
     * @throws Exception
     */
    @Test
    public void testSelectivityEffectOnGroupBy() throws Exception {
        secondRowContainsQuery("explain select count(*), c1,c2 from ts_low_cardinality where c1 = 1 group by c1,c2", "outputRows=10", methodWatcher);

        rowContainsQuery(new int[]{2,6}, "explain select count(*), mod(c1,3), upper(c2) from ts_low_cardinality where mod(c1,3) = 1 group by mod(c1,3), upper(c2)", methodWatcher,
                "outputRows=6", "IndexScan[TS_LOW_CARDINALITY_EXPR_IX_2");  // 2 rows
    }


    @Test
    public void testMonthSelectivity() throws Exception {
        secondRowContainsQuery("explain select count(*), month(c3) from ts_low_cardinality group by month(c3)", "outputRows=5", methodWatcher);
        secondRowContainsQuery("explain select count(*), month(c3) from ts_high_cardinality group by month(c3)", "outputRows=12", methodWatcher);
    }

    @Test
    public void testQuarterSelectivity() throws Exception {
        secondRowContainsQuery("explain select count(*), quarter(c3) from ts_low_cardinality group by quarter(c3)", "outputRows=4", methodWatcher);
        secondRowContainsQuery("explain select count(*), quarter(c3) from ts_high_cardinality group by quarter(c3)", "outputRows=4", methodWatcher);
    }

    @Test
    public void testConcatenationSupport() throws Exception {
        secondRowContainsQuery("explain select count(*), c2||c2 from TS_LOW_CARDINALITY group by c2||c2", "outputRows=5", methodWatcher);
    }

    @Test
    // DB-3414
    public void testCastSupport() throws Exception {
        secondRowContainsQuery("explain select (cast (c1 as char(2))), count(*) from TS_LOW_CARDINALITY group by (cast (c1 as char(2)))", "outputRows=5", methodWatcher);
    }

}
