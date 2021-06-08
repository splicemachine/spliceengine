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
import org.apache.logging.log4j.Logger;

import static java.lang.String.format;

public class ExecutionBenchmark extends Benchmark {
    protected static final Logger LOG = org.apache.logging.log4j.LogManager.getLogger(IndexLookupBenchmark.class);

    protected static final String BASE_TABLE = "BASE_TABLE";
    protected static final String BASE_TABLE_IDX = "BASE_TABLE_IDX";
    protected static final int NUM_EXECS = 5;
    protected static final int NUM_WARMUP_RUNS = 3;
    protected static final int NUM_CONNECTIONS = 10;

    protected static final int TABLE_DEF = 0;
    protected static final int INDEX_DEF = 1;
    protected static final int INSERT_PARAM = 3;
    protected static final int UPDATE_PARAM = 4;

    static final String STAT_ERROR = "ERROR";
    static final String STAT_DELETE = "DELETE";
    static final String STAT_UPDATE = "UPDATE";
    static final String STAT_INSERT = "INSERT";

    protected static Connection testConnection;
    protected Statement testStatement;

    static AtomicInteger curSize = new AtomicInteger(0);

    protected Connection makeConnection(SpliceSchemaWatcher spliceSchemaWatcher) throws SQLException {
        Connection connection = SpliceNetConnection.getDefaultConnection();
        connection.setSchema(spliceSchemaWatcher.schemaName);
        connection.setAutoCommit(true);
        return connection;
    }

    protected void createIndexes(Statement statement, int numIndexTables, String indexDefStr) {
        try {
            LOG.info("Creating " + numIndexTables + " indexes");
            for (int i = 1; i <= numIndexTables; i++) {
                statement.execute("CREATE INDEX " + BASE_TABLE_IDX + "_" + i + " on " + BASE_TABLE + indexDefStr);
            }
        }
        catch (Exception e) {
            LOG.error(e);
        }
    }

    protected void refreshTableState(Statement statement, String schema) throws Exception {
        statement.execute(format("call syscs_util.syscs_flush_table('%s', '%s')", schema, BASE_TABLE));
        statement.execute("ANALYZE SCHEMA " + schema);
        statement.execute("call SYSCS_UTIL.VACUUM()");
    }

    protected static String getColumnDef(int numColumns, int defStrType) {
        StringBuilder sb = new StringBuilder();
        if (defStrType == INSERT_PARAM) sb.append(" values ");
        if (defStrType != UPDATE_PARAM) sb.append(" (");
        for (int i = 0; i < numColumns; i++) {
            if (defStrType == UPDATE_PARAM && i > 1)
                sb.append(", ");
            else if (defStrType != UPDATE_PARAM && i != 0) {
                sb.append(", ");
            }

            switch (defStrType) {
                case TABLE_DEF:
                    sb.append("col_");
                    sb.append(i);
                    sb.append(" int");
                    break;
                case INDEX_DEF:
                    sb.append("col_");
                    sb.append(i);
                    break;
                case INSERT_PARAM:
                    sb.append("?");
                    break;
                case UPDATE_PARAM:
                    if (i != 0) {
                        // skip col_1 because it's our param in update statement
                        sb.append("col_");
                        sb.append(i);
                        sb.append(" = ");
                        sb.append("col_");
                        sb.append(i);
                        sb.append(" + 1");
                    }
                    break;
                default:
                    assert false : "unknown definition type";
                    break;
            }
        }
        if (defStrType != UPDATE_PARAM) sb.append(")");
        return sb.toString();
    }

    protected void populateTables(Connection connection, int size, int batchSize, int numTableColumns,
        String insertParamStr, int concurrency) {
        try (PreparedStatement insert1 = connection.prepareStatement("INSERT INTO " + BASE_TABLE + insertParamStr)) {
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
