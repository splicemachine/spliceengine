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

public class DistinctScanSelectivityIT extends SpliceUnitTest {
    public static final String CLASS_NAME = DistinctScanSelectivityIT.class.getSimpleName().toUpperCase();
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
    public void testDistinctCount() throws Exception {
        firstRowContainsQuery("explain select distinct * from ts_low_cardinality", "outputRows=125", methodWatcher);
        firstRowContainsQuery("explain select distinct c1 from ts_low_cardinality", "outputRows=5", methodWatcher);
        firstRowContainsQuery("explain select distinct c1, c2 from ts_low_cardinality", "outputRows=25", methodWatcher);
        firstRowContainsQuery("explain select distinct c1,c2,c3 from ts_low_cardinality", "outputRows=125", methodWatcher);
        firstRowContainsQuery("explain select distinct c1,c2,c3,c4 from ts_low_cardinality", "outputRows=125", methodWatcher);

        // Add For Non Stats?
    }


    @Test
    @Ignore("DB-3622")
    public void testDistinctNodeCount() throws Exception {
        firstRowContainsQuery("explain select distinct month(c3) from ts_low_cardinality", "outputRows=5", methodWatcher);
        firstRowContainsQuery("explain select distinct month(c3) from ts_high_cardinality", "outputRows=12", methodWatcher);
    }

}