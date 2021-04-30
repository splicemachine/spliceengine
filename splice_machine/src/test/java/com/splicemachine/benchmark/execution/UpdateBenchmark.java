package com.splicemachine.benchmark.execution;

import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.test.Benchmark;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static org.junit.Assert.assertTrue;

@Category(Benchmark.class)
@RunWith(Parameterized.class)
public class UpdateBenchmark extends ExecutionBenchmark {
    private static final String SCHEMA = UpdateBenchmark.class.getSimpleName();

    private static final int TABLE_DEF = 0;
    private static final int INDEX_DEF = 1;
    private static final int UPDATE_DEF = 2;
    private static final int INSERT_PARAM = 3;
    private static final int UPDATE_PARAM = 4;

    private final int numTableColumns;
    private final int numIndexTables;
    private final int tableSize;
    private final boolean onOlap;

    private final int batchSize;
    private final String tableDefStr;
    private final String indexDefStr;
    private final String updateDefStr;
    private final String insertParamStr;
    private final String updateParamStr;

    public UpdateBenchmark(int numTableColumns, int numIndexColumns, int numIndexTables, int tableSize,
        boolean onOlap) {
        this.numTableColumns = Math.max(numTableColumns, 1);
        this.numIndexTables = numIndexTables;
        this.tableSize = tableSize;
        this.onOlap = onOlap;
        this.batchSize = Math.min(tableSize, 1000);

        int realIndexColumns = Math.max(Math.min(numIndexColumns, numTableColumns), 1);

        this.tableDefStr = getColumnDef(this.numTableColumns, TABLE_DEF);
        this.indexDefStr = getColumnDef(realIndexColumns, INDEX_DEF);
        this.updateDefStr = getColumnDef(this.numTableColumns, UPDATE_DEF);
        this.insertParamStr = getColumnDef(this.numTableColumns, INSERT_PARAM);
        this.updateParamStr = getColumnDef(this.numTableColumns, UPDATE_PARAM);
    }

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    static AtomicInteger curSize = new AtomicInteger(0);

    @Before
    public void setUp() throws Exception {
        getInfo();

        LOG.info("Create tables");
        testConnection = makeConnection(spliceSchemaWatcher, false);
        testStatement = testConnection.createStatement();
        testStatement.execute("CREATE TABLE " + BASE_TABLE + tableDefStr);

        createIndexes(numIndexTables, indexDefStr);

        curSize.set(0);
        runBenchmark(NUM_CONNECTIONS, () -> populateTables(spliceSchemaWatcher, tableSize, curSize, batchSize, numTableColumns, insertParamStr));

        testStatement.execute(String.format("call syscs_util.syscs_flush_table('%s', '%s')", SCHEMA, BASE_TABLE));
        LOG.info("Collect statistics");
        try (ResultSet rs = testStatement.executeQuery("ANALYZE SCHEMA " + SCHEMA)) {
            assertTrue(rs.next());
        }
    }

    @After
    public void tearDown() throws Exception {
        testStatement.execute("DROP TABLE " + BASE_TABLE);
        testStatement.close();
        testConnection.close();
    }

    private void benchmark(boolean singleRowUpdate) {
        String sqlText = String.format("UPDATE %s SET %s = %s WHERE col_0 = ?",
            BASE_TABLE,
            updateDefStr,
            updateParamStr);

        try (Connection conn = makeConnection(spliceSchemaWatcher, true)) {
            try (PreparedStatement query = conn.prepareStatement(sqlText)) {
                for (int i = 0; i < NUM_WARMUP_RUNS; ++i) {
                    if (singleRowUpdate) {
                        for (int k = 0; k < tableSize; k++) {
                            executeUpdate(true, query, 0, 1);
                        }
                    }
                    else {
                        for (int k = 0; k < tableSize; k++) {
                            executeUpdate(true, query, 0, k);
                        }
                    }
                }

                for (int i = 0; i < ExecutionBenchmark.NUM_EXECS; ++i) {
                    if (singleRowUpdate) {
                        for (int k = 0; k < tableSize; k++) {
                            executeUpdate(false, query, i, 1);
                        }
                    } else {
                        for (int k = 0; k < tableSize; k++) {
                            executeUpdate(false, query, i, k);
                        }
                    }
                }
            }
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
        }
    }

    private void executeUpdate(boolean warmup, PreparedStatement query, int i, int k) throws SQLException {
        query.setInt(1, k);

        long start = System.currentTimeMillis();
        try {
            int rc = query.executeUpdate();

            if (!warmup) {
                if (rc != 1) {
                    updateStats(STAT_ERROR);
                }
                else {
                    long stop = System.currentTimeMillis();
                    updateStats(STAT_UPDATE, stop - start);
                }
            }
        }
        catch (SQLException ex) {
            LOG.error("ERROR execution " + i + " of indexLookup benchmark on " + BASE_TABLE + ": " + ex.getMessage());
            updateStats(STAT_ERROR);
        }
    }

    @Parameterized.Parameters
    public static Collection testParams() {
        return Arrays.asList(new Object[][] {
            {50, 2, 1, 5000, false},
            {50, 2, 5, 5000, false},
            {50, 2, 10, 5000, false},
            {50, 2, 20, 5000, false},
            {50, 2, 50, 5000, false},
            {50, 5, 1, 5000, false},
            {50, 5, 5, 5000, false},
            {50, 5, 10, 5000, false},
            {50, 5, 20, 5000, false},
            {50, 5, 50, 5000, false},

            {500, 2, 1, 5000, false},
            {500, 2, 5, 5000, false},
            {500, 2, 10, 5000, false},
            {500, 2, 20, 5000, false},
            {500, 2, 50, 5000, false},
            {500, 5, 1, 5000, false},
            {500, 5, 5, 5000, false},
            {500, 5, 10, 5000, false},
            {500, 5, 20, 5000, false},
            {500, 5, 50, 5000, false},

            {1000, 2, 1, 5000, false},
            {1000, 2, 5, 5000, false},
            {1000, 2, 10, 5000, false},
            {1000, 2, 20, 5000, false},
            {1000, 2, 50, 5000, false},
            {1000, 5, 1, 5000, false},
            {1000, 5, 5, 5000, false},
            {1000, 5, 10, 5000, false},
            {1000, 5, 20, 5000, false},
            {1000, 5, 50, 5000, false},
        });
    }

    /* Warming up would bring data into hbase block cache. To run cold only benchmarks,
     * hbase block cache must be disabled first:
     * hfile.block.cache.size = 0
     */

    @Test
    public void updateSingleRow() throws Exception {
        LOG.info("update-single-row");
        runBenchmark(1, () -> benchmark(onOlap));
    }

    @Test
    public void updateAllRows() throws Exception {
        LOG.info("update-multiple-rows");
        runBenchmark(1, () -> benchmark(onOlap));
    }
}
