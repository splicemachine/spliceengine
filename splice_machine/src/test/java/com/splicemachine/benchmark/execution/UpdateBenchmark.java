package com.splicemachine.benchmark.execution;

import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.Benchmark;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category(Benchmark.class)
@RunWith(Parameterized.class)
public class UpdateBenchmark extends ExecutionBenchmark {
    private static final String SCHEMA = UpdateBenchmark.class.getSimpleName();

    private final int numTableColumns;
    private final int numIndexTables;
    private final int dataSize;
    private final boolean onOlap;

    private final int batchSize;
    private final String tableDefStr;
    private final String indexDefStr;
    private final String insertParamStr;
    private final String updateParamStr;

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    public UpdateBenchmark(int numTableColumns, int numIndexColumns, int numIndexTables, int dataSize,
        boolean onOlap) {
        this.numTableColumns = Math.max(numTableColumns, 1);
        this.numIndexTables = numIndexTables;
        this.dataSize = dataSize;
        this.onOlap = onOlap;
        this.batchSize = Math.min(dataSize, 1000);

        int realIndexColumns = Math.max(Math.min(numIndexColumns, numTableColumns), 1);

        this.tableDefStr = getColumnDef(this.numTableColumns, TABLE_DEF);
        this.indexDefStr = getColumnDef(realIndexColumns, INDEX_DEF);
        this.insertParamStr = getColumnDef(this.numTableColumns, INSERT_PARAM);
        this.updateParamStr = getColumnDef(this.numTableColumns, UPDATE_PARAM);
    }

    @Before
    public void setUp() throws Exception {
        getInfo();

        LOG.info("Setup connection and creating tables");
        testConnection = makeConnection(spliceSchemaWatcher);
        testStatement = testConnection.createStatement();
        testStatement.execute("CREATE TABLE " + BASE_TABLE + tableDefStr);

        createIndexes(testStatement, numIndexTables, indexDefStr);

        LOG.info("Populating tables");
        curSize.set(0);
        runBenchmark(1, () -> populateTables(testConnection, dataSize, batchSize, numTableColumns, insertParamStr, curSize.getAndAdd(dataSize)));

        refreshTableState(testStatement, SCHEMA);
    }

    @After
    public void tearDown() throws Exception {
        spliceSchemaWatcher.cleanSchemaObjects();
        testStatement.close();
        testConnection.close();
    }

    private void benchmark(boolean singleRowUpdate) {
        int concurrency = curSize.getAndAdd(dataSize);
        String sqlText = String.format("UPDATE %s SET %s WHERE col_0 = ?",
            BASE_TABLE,
            updateParamStr);

        try (PreparedStatement query = testConnection.prepareStatement(sqlText)) {
            for (int i = 0; i < NUM_WARMUP_RUNS; ++i) {
                if (singleRowUpdate) {
                    for (int k = concurrency; k < concurrency + dataSize; k++) {
                        executeUpdate(true, query, 0, 1);
                    }
                }
                else {
                    for (int k = concurrency; k < concurrency + dataSize; k++) {
                        executeUpdate(true, query, 0, k);
                    }
                }
            }

            for (int i = 0; i < ExecutionBenchmark.NUM_EXECS; ++i) {
                if (singleRowUpdate) {
                    for (int k = concurrency; k < concurrency + dataSize; k++) {
                        executeUpdate(false, query, i, 1);
                    }
                }
                else {
                    for (int k = concurrency; k < concurrency + dataSize; k++) {
                        executeUpdate(false, query, i, k);
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
            {50, 1, 50, 5000, false},
            {50, 10, 50, 5000, false},

            {1000, 1, 50, 5000, false},
            {1000, 10, 50, 5000, false},

            {1500, 1, 50, 5000, false},
            {1500, 10, 50, 5000, false},
        });
    }

    /* Warming up would bring data into hbase block cache. To run cold only benchmarks,
     * hbase block cache must be disabled first:
     * hfile.block.cache.size = 0
     */

    @Test
    public void updateSingleRow() throws Exception {
        LOG.info("update-single-row");
        runBenchmark(1, () -> benchmark(true));
    }

    @Test
    public void updateAllRows() throws Exception {
        LOG.info("update-multiple-rows");
        runBenchmark(1, () -> benchmark(false));
    }
}
