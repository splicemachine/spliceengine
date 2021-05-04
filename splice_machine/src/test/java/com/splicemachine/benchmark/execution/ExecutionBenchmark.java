package com.splicemachine.benchmark.execution;

import com.splicemachine.benchmark.IndexLookupBenchmark;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.test.Benchmark;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

import static java.lang.String.format;

public class ExecutionBenchmark extends Benchmark {
    protected static final Logger LOG = Logger.getLogger(IndexLookupBenchmark.class);

    protected static final String BASE_TABLE = "BASE_TABLE";
    protected static final String BASE_TABLE_IDX = "BASE_TABLE_IDX";
    protected static final int NUM_EXECS = 5;
    protected static final int NUM_WARMUP_RUNS = 5;
    protected static final int NUM_CONNECTIONS = 10;

    protected static final int TABLE_DEF = 0;
    protected static final int INDEX_DEF = 1;
    protected static final int UPDATE_DEF = 2;
    protected static final int INSERT_PARAM = 3;
    protected static final int UPDATE_PARAM = 4;

    static final String STAT_ERROR = "ERROR";
    static final String STAT_DELETE = "DELETE";
    static final String STAT_UPDATE = "UPDATE";
    static final String STAT_INSERT = "INSERT";

    protected static Connection testConnection;

    static AtomicInteger curSize = new AtomicInteger(0);

    protected void createIndexes(SpliceWatcher spliceWatcher, int numIndexTables, String indexDefStr) {
        try {
            LOG.info("Creating " + numIndexTables + " indexes");
            for (int i = 1; i <= numIndexTables; i++) {
                spliceWatcher.execute("CREATE INDEX " + BASE_TABLE_IDX + "_" + i + " on " + BASE_TABLE + indexDefStr);
            }
        }
        catch (Exception e) {
            LOG.error(e);
        }
    }

    protected void refreshTableState(SpliceWatcher spliceWatcher, String schema) throws Exception {
        spliceWatcher.execute(format("call syscs_util.syscs_flush_table('%s', '%s')", schema, BASE_TABLE));
        spliceWatcher.execute("ANALYZE SCHEMA " + schema);
        spliceWatcher.execute("call SYSCS_UTIL.VACUUM()");
    }

    protected static String getColumnDef(int numColumns, int defStrType) {
        StringBuilder sb = new StringBuilder();
        if (defStrType == INSERT_PARAM) {
            sb.append(" values ");
        }
        sb.append(" (");
        for (int i = 0; i < numColumns; i++) {
            if (i != 0) {
                sb.append(", ");
            }

            switch (defStrType) {
                case TABLE_DEF:
                    sb.append("col_");
                    sb.append(i);
                    sb.append(" int");
                    break;
                case INDEX_DEF:
                case UPDATE_DEF:
                    sb.append("col_");
                    sb.append(i);
                    break;
                case INSERT_PARAM:
                    sb.append("?");
                    break;
                case UPDATE_PARAM:
                    sb.append("col_");
                    sb.append(i);
                    sb.append(" + 1");
                    break;
                default:
                    assert false : "unknown definition type";
                    break;
            }
        }
        sb.append(")");
        return sb.toString();
    }

    protected void populateTables(SpliceWatcher spliceWatcher, int size, int batchSize, int numTableColumns,
        String insertParamStr, int concurrency) {
        try (PreparedStatement insert1 = spliceWatcher.prepareStatement("INSERT INTO " + BASE_TABLE + insertParamStr)) {
            int newSize = concurrency;
            for (; ; ) {
                if (newSize >= concurrency + size)
                    break;
                for (int i = newSize; i < newSize + batchSize; ++i) {
                    for (int j = 1; j <= numTableColumns; ++j) {
                        insert1.setInt(j, i);
                    }
                    insert1.addBatch();
                }
                long start = System.currentTimeMillis();
                int[] counts = insert1.executeBatch();
                long end = System.currentTimeMillis();
                int count = 0;
                for (int c : counts)
                    count += c;
                if (count != batchSize) {
                    updateStats(STAT_ERROR);
                }
                if (count > 0) {
                    updateStats(STAT_INSERT, count, end - start);
                }
                newSize += batchSize;
            }
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
        }
    }
}
