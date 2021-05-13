package com.splicemachine.benchmark.execution;

import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.test.Benchmark;
import java.sql.PreparedStatement;
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

@Category(Benchmark.class)
@RunWith(Parameterized.class)
public class DeleteBenchmark extends ExecutionBenchmark {
    private static final String SCHEMA = DeleteBenchmark.class.getSimpleName();

    private final int numTableColumns;
    private final int numIndexTables;
    private final int dataSize;
    private final boolean onOlap;

    private final int batchSize;
    private final String tableDefStr;
    private final String indexDefStr;
    private final String insertParamStr;

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    public DeleteBenchmark(int numTableColumns, int numIndexColumns, int numIndexTables, int dataSize,
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
    }

    static AtomicInteger curSize = new AtomicInteger(0);

    @Before
    public void setUp() throws Exception {
        getInfo();

        LOG.info("Setup connection and creating tables");
        testConnection = makeConnection(spliceSchemaWatcher);
        testStatement = testConnection.createStatement();
        testStatement.execute("CREATE TABLE " + BASE_TABLE + tableDefStr);

        createIndexes(testStatement, numIndexTables, indexDefStr);

        refreshTableState(testStatement, SCHEMA);
    }

    @After
    public void tearDown() throws Exception {
        spliceSchemaWatcher.cleanSchemaObjects();
        testStatement.close();
        testConnection.close();
    }

    private void benchmark() {
        int concurrency = curSize.getAndAdd(dataSize);
        String sqlText = String.format("DELETE FROM %s WHERE col_0 = ?",
            BASE_TABLE);

        try {
            for (int i = 0; i < ExecutionBenchmark.NUM_EXECS; ++i) {
                populateTables(testConnection, dataSize, batchSize, numTableColumns, insertParamStr, concurrency);
                testStatement.executeUpdate("call SYSCS_UTIL.VACUUM()");

                try (PreparedStatement query = testConnection.prepareStatement(sqlText)) {
                    for (int k = concurrency; k < concurrency + dataSize; k++) {
                        query.setInt(1, k);

                        long start = System.currentTimeMillis();
                        try {
                            int rc = query.executeUpdate();
                            if (rc != 1) {
                                updateStats(STAT_ERROR);
                            }
                            else {
                                long stop = System.currentTimeMillis();
                                updateStats(STAT_DELETE, stop - start);
                            }
                        }
                        catch (SQLException ex) {
                            LOG.error("ERROR execution " + i + " of indexLookup benchmark on " + BASE_TABLE + ": " + ex.getMessage());
                            updateStats(STAT_ERROR);
                        }
                    }
                }
            }
        } catch (Exception e) {
            LOG.error("Error occurs during evaluating benchmark", e);
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
    public void deleteFromTable() throws Exception {
        LOG.info("delete");
        runBenchmark(2, this::benchmark);
    }
}
