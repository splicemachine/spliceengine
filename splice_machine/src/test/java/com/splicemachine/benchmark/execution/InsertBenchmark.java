package com.splicemachine.benchmark.execution;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.Benchmark;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.RuleChain;
import org.junit.rules.TestRule;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import static java.lang.String.format;

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

    public static SpliceWatcher spliceWatcher = new SpliceWatcher(SCHEMA);
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    @Rule
    public TestRule chain = RuleChain.outerRule(spliceWatcher)
        .around(spliceSchemaWatcher);

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
        spliceWatcher.setConnection(spliceWatcher.connectionBuilder().useOLAP(false).schema(SCHEMA).build());
        spliceWatcher.execute("CREATE TABLE " + BASE_TABLE + tableDefStr);

        createIndexes(spliceWatcher, numIndexTables, indexDefStr);
    }

    private void benchmark(boolean isBatch) {
        try {
            // warmup
            if (isBatch)
                runBatchInserts(true, curSize.getAndAdd(dataSize));
            else
                runInserts(true, curSize.getAndAdd(dataSize));

            if (isBatch)
                runBatchInserts(false, curSize.getAndAdd(dataSize));
            else
                runInserts(false, curSize.getAndAdd(dataSize));
        } catch (SQLException e) {
            LOG.error("Error occurs while initializing test connection", e);
        }
    }

    private void runInserts(boolean warmup, int concurrency) throws SQLException {
        spliceWatcher.setConnection(spliceWatcher.connectionBuilder().useOLAP(onOlap).schema(SCHEMA).build());

        try (PreparedStatement insert1 = spliceWatcher.prepareStatement("INSERT INTO " + BASE_TABLE + insertParamStr)) {
            for (int i = 0; i < ExecutionBenchmark.NUM_WARMUP_RUNS; ++i) {
                refreshTableState(spliceWatcher, SCHEMA);

                long start = System.currentTimeMillis();
                for (int j = concurrency; j < concurrency + dataSize; ++j) {
                    for (int k = 1; k <= numTableColumns; ++k) {
                        insert1.setInt(k, j);
                    }
                    insert1.execute();
                }
                long end = System.currentTimeMillis();

                if (!warmup) {
                    updateStats(STAT_INSERT, dataSize, end - start);
                }
            }
        }
        catch (Exception e) {
            updateStats(STAT_ERROR);
        }
    }

    private void runBatchInserts(boolean warmup, int concurrency) throws SQLException {
        int batchSize = Math.min(dataSize, 1000);
        spliceWatcher.setConnection(spliceWatcher.connectionBuilder().useOLAP(onOlap).schema(SCHEMA).build());

        try (PreparedStatement insert1 = spliceWatcher.prepareStatement("INSERT INTO " + BASE_TABLE + insertParamStr)) {
            for (int t = 0; t < ExecutionBenchmark.NUM_WARMUP_RUNS; ++t) {
                refreshTableState(spliceWatcher, SCHEMA);

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

                    if (!warmup) {
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
