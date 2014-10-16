package com.splicemachine.derby.impl.sql.execute.operations;

import com.splicemachine.derby.test.framework.SpliceDataWatcher;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceTableWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.test.SerialTest;
import com.splicemachine.test.SlowTest;
import com.splicemachine.test_dao.StatementHistory;
import com.splicemachine.test_dao.StatementHistoryDAO;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.Description;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.lang.String.format;
import static org.junit.Assert.*;

/**
 * SerialTest because it clears the statement history table, SlowTests because it performs manual splits.
 */
@Category(value = {SerialTest.class, SlowTest.class})
public class MultiRegionIT {

    private static final String SCHEMA_NAME = MultiRegionIT.class.getSimpleName().toUpperCase();
    private static final SpliceWatcher spliceClassWatcher = new SpliceWatcher(SCHEMA_NAME);
    private static final String TABLE_NAME = "TAB";
    private static final SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA_NAME);
    private static final SpliceTableWatcher spliceTableWatcher = new SpliceTableWatcher(TABLE_NAME, SCHEMA_NAME, "(I INT, D DOUBLE)");

    @ClassRule
    public static TestRule chain = RuleChain.outerRule(spliceClassWatcher)
            .around(spliceSchemaWatcher)
            .around(spliceTableWatcher).around(new SpliceDataWatcher() {
                @Override
                protected void starting(Description description) {
                    PreparedStatement ps;
                    try {
                        ps = spliceClassWatcher.prepareStatement(format("insert into %s (i, d) values (?, ?)", TABLE_NAME));
                        for (int j = 0; j < 100; ++j) {
                            for (int i = 0; i < 10; i++) {
                                ps.setInt(1, i);
                                ps.setDouble(2, i * 1.0);
                                ps.execute();
                            }
                        }
                        spliceClassWatcher.splitTable(TABLE_NAME, SCHEMA_NAME, 250);
                        spliceClassWatcher.splitTable(TABLE_NAME, SCHEMA_NAME, 500);
                        spliceClassWatcher.splitTable(TABLE_NAME, SCHEMA_NAME, 750);

                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });

    @Rule
    public SpliceWatcher methodWatcher = new SpliceWatcher(SCHEMA_NAME);

    @Test
    public void testStddevAndAutoTrace() throws Exception {
        Connection conn = methodWatcher.createConnection();
        StatementHistoryDAO statementHistoryDAO = new StatementHistoryDAO(conn);

        conn.createStatement().execute("call SYSCS_UTIL.SYSCS_PURGE_XPLAIN_TRACE()");
        conn.commit();

        int numRegions = getNumOfRegions(SCHEMA_NAME, TABLE_NAME);

        Double popValue = methodWatcher.query(format("select stddev_pop(i) from %s", TABLE_NAME));
        assertEquals(2.8, popValue, .5);

        Double sampleValue = methodWatcher.query(format("select stddev_samp(i) from %s", TABLE_NAME));
        assertEquals(2.8, sampleValue, .5);

        if (numRegions > 3) {
            StatementHistory hist1 = statementHistoryDAO.findStatement("stddev_samp", 20, TimeUnit.SECONDS);
            StatementHistory hist2 = statementHistoryDAO.findStatement("stddev_pop", 20, TimeUnit.SECONDS);
            assertTrue("hist1=" + hist1 + " hist2=" + hist2, hist1 != null && hist2 != null);
        }
    }

    @Test
    public void testDistinctCount() throws Exception {
        Long count = methodWatcher.query(format("select count(distinct i) from %s", TABLE_NAME));
        assertEquals(10, count.intValue());
    }

    @Test
    public void testAutoTraceOff() throws Exception {
        Connection conn = methodWatcher.createConnection();
        StatementHistoryDAO statementHistoryDAO = new StatementHistoryDAO(conn);

        conn.createStatement().execute("call SYSCS_UTIL.SYSCS_PURGE_XPLAIN_TRACE()");

        // turn OFF auto trace
        conn.createStatement().execute("call SYSCS_UTIL.SYSCS_SET_AUTO_TRACE(0)");

        Double sampleValue = methodWatcher.query(format("select stddev_samp(i) from %s", TABLE_NAME));
        assertEquals(2.8, sampleValue, .5);

        // turn ON auto trace
        conn.createStatement().execute("call SYSCS_UTIL.SYSCS_SET_AUTO_TRACE(1)");

        StatementHistory history = statementHistoryDAO.findStatement("stddev_samp", 6, TimeUnit.SECONDS);
        assertNull("expected not to find it because auto trace was off", history);
    }

    private int getNumOfRegions(String schemaName, String tableName) throws Exception {
        long conglomId = spliceClassWatcher.getConglomId(tableName, schemaName);
        HBaseAdmin admin = SpliceUtils.getAdmin();
        List<HRegionInfo> regions = admin.getTableRegions(Bytes.toBytes(Long.toString(conglomId)));
        return regions.size();
    }
}
