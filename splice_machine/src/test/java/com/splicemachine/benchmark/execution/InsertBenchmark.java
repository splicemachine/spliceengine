package com.splicemachine.benchmark.execution;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.test.Benchmark;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@Category(Benchmark.class)
@RunWith(Parameterized.class)
public class InsertBenchmark extends ExecutionBenchmark {
    private static final String SCHEMA = InsertBenchmark.class.getSimpleName();

    private final int numTableColumns;
    private final int numIndexTables;
    private final int dataSize;
    private final boolean onOlap;

    private final String tableDefStr;
    private final String indexDefStr;
    private final String insertParamStr;

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    public InsertBenchmark(int numTableColumns, int numIndexColumns, int numIndexTables, int dataSize, boolean onOlap) {
        this.numTableColumns = Math.max(numTableColumns, 1);
        this.numIndexTables = numIndexTables;
        this.dataSize = dataSize;
        this.onOlap = onOlap;

        int realIndexColumns = Math.max(Math.min(numIndexColumns, numTableColumns), 1);

        this.tableDefStr = getColumnDef(this.numTableColumns, TABLE_DEF);
        this.indexDefStr = getColumnDef(realIndexColumns, INDEX_DEF);
        this.insertParamStr = getColumnDef(this.numTableColumns, INSERT_PARAM);
    }

    @Before
    public void setUp() throws Exception {
        getInfo();

        LOG.info("Setup connection and creating tables");
        testConnection = makeConnection(spliceSchemaWatcher);
        testStatement = testConnection.createStatement();
        testStatement.execute("CREATE TABLE " + BASE_TABLE + tableDefStr);

        createIndexes(testStatement, numIndexTables, indexDefStr);
    }

    @After
    public void tearDown() throws Exception {
        spliceSchemaWatcher.cleanSchemaObjects();
        testStatement.close();
        testConnection.close();
        reportStats();
    }

    private void benchmark(boolean isBatch) {
        try {
            // warmup
            if (isBatch)
                runBatchInserts(NUM_WARMUP_RUNS, curSize.getAndAdd(dataSize), false);
            else
                runInserts(NUM_WARMUP_RUNS, curSize.getAndAdd(dataSize), false);

            if (isBatch)
                runBatchInserts(NUM_EXECS, curSize.getAndAdd(dataSize), true);
            else
                runInserts(NUM_EXECS, curSize.getAndAdd(dataSize), true);
        } catch (SQLException e) {
            LOG.error("Error occurs while initializing test connection", e);
        }
    }

    private void runInserts(int runs, int concurrency, boolean collectStats) throws SQLException {
        try (PreparedStatement insert1 = testConnection.prepareStatement("INSERT INTO " + BASE_TABLE + insertParamStr)) {
            for (int i = 0; i < runs; ++i) {
                refreshTableState(testStatement, SCHEMA);

                long start = System.currentTimeMillis();
                for (int j = concurrency; j < concurrency + dataSize; ++j) {
                    for (int k = 1; k <= numTableColumns; ++k) {
                        insert1.setInt(k, j);
                    }
                    insert1.execute();
                }
                long end = System.currentTimeMillis();

                if (!collectStats) {
                    updateStats(STAT_INSERT, dataSize, end - start);
                }
            }
        }
        catch (Exception e) {
            updateStats(STAT_ERROR);
        }
    }

    private void runBatchInserts(int runs, int concurrency, boolean collectStats) throws SQLException {
        int batchSize = Math.min(dataSize, 1000);

        try (PreparedStatement insert1 = testConnection.prepareStatement("INSERT INTO " + BASE_TABLE + insertParamStr)) {
            for (int t = 0; t < runs; ++t) {
                refreshTableState(testStatement, SCHEMA);

                for (int k = 0; k < dataSize / batchSize; ++k) {
                    for (int i = concurrency + k * batchSize; i < concurrency + (k + 1) * batchSize; ++i) {
                        for (int j = 1; j <= numTableColumns; ++j) {
                            insert1.setInt(j, i);
                        }
                        insert1.addBatch();
                    }
                    long start = System.currentTimeMillis();
                    int[] counts = insert1.executeBatch();
                    long end = System.currentTimeMillis();

                    if (!collectStats) {
                        int count = 0;
                        for (int c : counts)
                            count += c;
                        if (count != batchSize) {
                            updateStats(STAT_ERROR);
                        }
                        if (count > 0) {
                            updateStats(STAT_INSERT, count, end - start);
                        }
                    }
                }
            }
        }
        catch (Exception e) {
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
    public void insertIntoIndexedTable() throws Exception {
        LOG.info("insert");
        runBenchmark(1, () -> benchmark(false));
    }

    @Test
    public void insertBatchIntoIndexedTable() throws Exception {
        LOG.info("insert-batch");
        runBenchmark(1, () -> benchmark(true));
    }
}
