package com.splicemachine.benchmark.execution;

import com.splicemachine.benchmark.IndexLookupBenchmark;
import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.test.Benchmark;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.log4j.Logger;

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
    protected Statement testStatement;

    protected void createIndexes(int numIndexTables, String indexDefStr) {
        try {
            LOG.info("Creating " + numIndexTables + " indexes");
            for (int i = 1; i <= numIndexTables; i++) {
                testStatement.execute("CREATE INDEX " + BASE_TABLE_IDX + "_" + i + " on " + BASE_TABLE + indexDefStr);
            }
        }
        catch (SQLException e) {
            LOG.error(e);
        }
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

    static Connection makeConnection(SpliceSchemaWatcher spliceSchemaWatcher, boolean useOLAP) throws SQLException {
        Connection connection = SpliceNetConnection.newBuilder().useOLAP(useOLAP).build();
        connection.setSchema(spliceSchemaWatcher.schemaName);
        connection.setAutoCommit(true);
        return connection;
    }

    protected void populateTables(SpliceSchemaWatcher spliceSchemaWatcher, int size, AtomicInteger curSize, int batchSize, int numTableColumns, String insertParamStr) {
        try (Connection conn = makeConnection(spliceSchemaWatcher, false)) {
            try (PreparedStatement insert1 = conn.prepareStatement("INSERT INTO " + BASE_TABLE + insertParamStr)) {
                for (; ; ) {
                    int newSize = curSize.getAndAdd(batchSize);
                    if (newSize >= size) break;
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
                    for (int c : counts) count += c;
                    if (count != batchSize) {
                        updateStats(STAT_ERROR);
                    }
                    if (count > 0) {
                        updateStats(STAT_INSERT, count, end - start);
                    }
                }
            }
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
        }
    }
}
