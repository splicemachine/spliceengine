package com.splicemachine.benchmark.execution;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.test.Benchmark;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
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

import static org.junit.Assert.assertTrue;

@Category(Benchmark.class)
@RunWith(Parameterized.class)
public class InsertBenchmark extends ExecutionBenchmark {
    private static final String SCHEMA = InsertBenchmark.class.getSimpleName();

    private final int numTableColumns;
    private final int numIndexTables;
    private final int tableSize;
    private final boolean onOlap;

    private final String tableDefStr;
    private final String indexDefStr;
    private final String insertParamStr;

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    public InsertBenchmark(int numTableColumns, int numIndexColumns, int numIndexTables, int tableSize, boolean onOlap) {
        this.numTableColumns = Math.max(numTableColumns, 1);
        this.numIndexTables = numIndexTables;
        this.tableSize = tableSize;
        this.onOlap = onOlap;

        int realIndexColumns = Math.max(Math.min(numIndexColumns, numTableColumns), 1);

        this.tableDefStr = getColumnDef(this.numTableColumns, TABLE_DEF);
        this.indexDefStr = getColumnDef(realIndexColumns, INDEX_DEF);
        this.insertParamStr = getColumnDef(this.numTableColumns, INSERT_PARAM);
    }

    @Before
    public void setUp() throws Exception {
        getInfo();

        LOG.info("Create tables");
        testConnection = makeConnection(spliceSchemaWatcher, onOlap);
        testStatement = testConnection.createStatement();
        testStatement.execute("CREATE TABLE " + BASE_TABLE + tableDefStr);
    }

    @After
    public void tearDown() throws Exception {
        testStatement.execute("DROP TABLE " + BASE_TABLE);
        testStatement.close();
        testConnection.close();
    }

    private void benchmark(int numIndexTables, boolean isBatch, boolean onOlap) {
        createIndexes(numIndexTables, indexDefStr);

        try (Connection conn = makeConnection(spliceSchemaWatcher, onOlap)) {
            // warmup
            if (isBatch) runBatchInserts(true, conn);
            else runInserts(true, conn);


            if (isBatch) runBatchInserts(false, conn);
            else runInserts(false, conn);
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
        }
    }

    private void refreshTableState() throws SQLException {
        testStatement.execute(String.format("call syscs_util.syscs_flush_table('%s', '%s')", SCHEMA, BASE_TABLE));
        try (ResultSet rs = testStatement.executeQuery("ANALYZE SCHEMA " + SCHEMA)) {
            assertTrue(rs.next());
        }
        testStatement.execute("call SYSCS_UTIL.VACUUM()");
    }

    private void runInserts(boolean warmup, Connection conn) {
        try (PreparedStatement insert1 = conn.prepareStatement("INSERT INTO " + BASE_TABLE + insertParamStr)) {
            for (int i = 0; i < ExecutionBenchmark.NUM_WARMUP_RUNS; ++i) {
                refreshTableState();

                long start = System.currentTimeMillis();
                for (int j = 0; j < tableSize; ++j) {
                    for (int k = 1; k <= numTableColumns; ++k) {
                        insert1.setInt(k, j);
                    }
                    insert1.execute();
                }
                long end = System.currentTimeMillis();

                if (!warmup) {
                    updateStats(STAT_INSERT, tableSize, end - start);
                }
            }
        }
        catch (Exception e) {
            updateStats(STAT_ERROR);
        }
    }

    private void runBatchInserts(boolean warmup, Connection conn) {
        int batchSize = Math.min(tableSize, 1000);

        try (PreparedStatement insert1 = conn.prepareStatement("INSERT INTO " + BASE_TABLE + insertParamStr)) {
            for (int t = 0; t < ExecutionBenchmark.NUM_WARMUP_RUNS; ++t) {
                refreshTableState();

                for (int k = 0; k < tableSize / batchSize; ++k) {
                    for (int i = k * batchSize; i < (k + 1) * batchSize; ++i) {
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
        runBenchmark(1, () -> benchmark(numIndexTables, false, onOlap));
    }

    @Test
    public void insertBatchIntoIndexedTable() throws Exception {
        LOG.info("insert-batch");
        runBenchmark(1, () -> benchmark(numIndexTables, true, onOlap));
    }
}
