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
        conn.commit();

        conn.createStatement().executeQuery(format(
                "call SYSCS_UTIL.COLLECT_SCHEMA_STATISTICS('%s',false)",
                spliceSchemaWatcher));
        conn.commit();
    }

    @Test
    @Ignore("DB-3729")
    public void testCoveringIndexScan() throws Exception {
        rowContainsQuery(3,"explain select c1 from ts_low_cardinality where c1 = 1","IndexScan[TS_LOW_CARDINALITY_IX_1",methodWatcher);
        rowContainsQuery(3,"explain select c1,c2 from ts_low_cardinality where c1 = 1","IndexScan[TS_LOW_CARDINALITY_IX_3",methodWatcher);
        rowContainsQuery(3,"explain select c2 from ts_low_cardinality where c2 = '1'","IndexScan[TS_LOW_CARDINALITY_IX_2",methodWatcher);
        rowContainsQuery(6,"explain select count(*) from ts_low_cardinality where c2 = '1'","IndexScan[TS_LOW_CARDINALITY_IX_2",methodWatcher);
    }

    @Test
    public void testSingleRowIndexLookup() throws Exception {
        rowContainsQuery(4,"explain select * from ts_high_cardinality where c1 = 1","IndexScan[TS_HIGH_CARDINALITY_IX_1",methodWatcher);
    }

    @Test
    public void testRangeIndexLookup() throws Exception {
        // 10/10000
        rowContainsQuery(4,"explain select * from ts_high_cardinality where c1 > 1 and c1 < 10","IndexScan[TS_HIGH_CARDINALITY_IX",methodWatcher);
        // 100/10000
        rowContainsQuery(4,"explain select * from ts_high_cardinality where c1 > 1 and c1 < 100","IndexScan[TS_HIGH_CARDINALITY_IX",methodWatcher);
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
    public void testNonCoveringIndexScan() throws Exception {

    }

    @Test
    public void testMostSelectiveIndexChosen() throws Exception {

    }

    @Test
    public void test1PercentRangeScan() throws Exception {

    }

    @Test
    public void test20PercentRangeScan() throws Exception {

    }


}
