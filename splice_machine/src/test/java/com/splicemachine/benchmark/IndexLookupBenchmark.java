/*
 * Copyright (c) 2021 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.benchmark;

import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.test.Benchmark;
import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

@Category(Benchmark.class)
@RunWith(Parameterized.class)
public class IndexLookupBenchmark extends Benchmark {

    private static final Logger LOG = Logger.getLogger(IndexLookupBenchmark.class);

    private static final String SCHEMA = IndexLookupBenchmark.class.getSimpleName();
    private static final String BASE_TABLE = "BASE_TABLE";
    private static final String BASE_TABLE_IDX = "BASE_TABLE_IDX";
    private static final int NUM_CONNECTIONS = 10;
    private static final int NUM_EXECS = 20;
    private static final int NUM_WARMUP_RUNS = 5;

    private static final int TABLE_DEF = 0;
    private static final int INDEX_DEF = 1;
    private static final int INSERT_PARAM = 2;

    private final int numTableColumns;
    private final int numIndexColumns;
    private final int tableSize;
    private final boolean onOlap;

    private final int batchSize;
    private final String tableDefStr;
    private final String indexDefStr;
    private final String insertParamStr;

    public IndexLookupBenchmark(int numTableColumns, int numIndexColumns, int tableSize, boolean onOlap) {
        this.numTableColumns = Math.max(numTableColumns, 1);
        this.numIndexColumns = Math.max(Math.min(numIndexColumns, numTableColumns), 1);
        this.tableSize = tableSize;
        this.onOlap = onOlap;

        this.batchSize = Math.min(tableSize, 1000);
        this.tableDefStr = getColumnDef(this.numTableColumns, TABLE_DEF);
        this.indexDefStr = getColumnDef(this.numIndexColumns, INDEX_DEF);
        this.insertParamStr = getColumnDef(this.numTableColumns, INSERT_PARAM);
    }

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher(SCHEMA);

    static Connection makeConnection() throws SQLException {
        Connection connection = SpliceNetConnection.getDefaultConnection();
        connection.setSchema(spliceSchemaWatcher.schemaName);
        connection.setAutoCommit(true);
        return connection;
    }

    static Connection testConnection;
    static Statement testStatement;

    @Before
    public void setUp() throws Exception {
        getInfo();

        LOG.info("Create tables");
        testConnection = makeConnection();
        testStatement = testConnection.createStatement();
        testStatement.execute("CREATE TABLE " + BASE_TABLE + tableDefStr);
        testStatement.execute("CREATE INDEX " + BASE_TABLE_IDX + " on " + BASE_TABLE + indexDefStr);

        curSize.set(0);
        runBenchmark(NUM_CONNECTIONS, () -> populateTables(tableSize));

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

    static final String STAT_ERROR = "ERROR";
    static final String STAT_CREATE = "CREATE";
    static final String INDEX_LOOKUP = "INDEX_LOOKUP";
    static final String INDEX_SCAN = "INDEX_SCAN";

    static AtomicInteger curSize = new AtomicInteger(0);

    private void populateTables(int size) {
        try (Connection conn = makeConnection()) {
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
                        updateStats(STAT_CREATE, count, end - start);
                    }
                }
            }
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
        }
    }

    private static String getColumnDef(int numColumns, int defStrType) {
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
                    sb.append("col_");
                    sb.append(i);
                    break;
                case INSERT_PARAM:
                    sb.append("?");
                    break;
                default:
                    assert false : "unknown definition type";
                    break;
            }
        }
        sb.append(")");
        return sb.toString();
    }

    private static int getRowCount(ResultSet rs) throws SQLException {
        assertTrue(rs.next());
        return rs.getInt(1);
    }

    private void benchmark(boolean isIndexLookup, int numExec, boolean warmUp, boolean onOlap) {
        String sqlText;
        String dataLabel;
        if (isIndexLookup) {
            sqlText = String.format("select count(col_%d) from %s --splice-properties index=%s, useSpark=%b", numIndexColumns, BASE_TABLE, BASE_TABLE_IDX, onOlap);
            dataLabel = INDEX_LOOKUP;
        } else {
            sqlText = String.format("select count(col_0) from %s --splice-properties index=%s, useSpark=%b", BASE_TABLE, BASE_TABLE_IDX, onOlap);
            dataLabel = INDEX_SCAN;
        }
        try (Connection conn = makeConnection()) {
            try (PreparedStatement query = conn.prepareStatement(sqlText)) {
                if (warmUp) {
                    for (int i = 0; i < NUM_WARMUP_RUNS; ++i) {
                        query.executeQuery();
                    }
                }

                if (isIndexLookup) {
                    executeQueryAndMatchLines(conn, "explain " + sqlText, Collections.singletonMap(6, new String[]{"IndexLookup"}));
                } else {
                    executeQueryAndMatchLines(conn, "explain " + sqlText, Collections.singletonMap(6, new String[]{"IndexScan"}));
                }

                for (int i = 0; i < numExec; ++i) {
                    long start = System.currentTimeMillis();
                    try (ResultSet rs = query.executeQuery()) {
                        if (getRowCount(rs) != tableSize) {
                            updateStats(STAT_ERROR);
                        } else {
                            long stop = System.currentTimeMillis();
                            updateStats(dataLabel, stop - start);
                        }
                    } catch (SQLException ex) {
                        LOG.error("ERROR execution " + i + " of indexLookup benchmark on " + BASE_TABLE + ": " + ex.getMessage());
                        updateStats(STAT_ERROR);
                    }
                }
            }
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
        }
    }

    @Parameterized.Parameters
    public static Collection testParams() {
        return Arrays.asList(new Object[][] {
                { 50, 2, 1000, false },
                { 50, 2, 5000, false },
                { 50, 2, 10000, false },
                { 50, 2, 15000, false },
                { 50, 2, 20000, false },
                { 50, 2, 25000, false },
                { 50, 2, 30000, false },
                { 50, 2, 35000, false },
                { 50, 2, 40000, false },
                { 50, 2, 45000, false },
                { 50, 2, 50000, false },
                { 50, 2, 100000, false },
                { 50, 2, 150000, false },
                { 50, 2, 200000, false },
                { 50, 2, 1000, true },
                { 50, 2, 5000, true },
                { 50, 2, 10000, true },
                { 50, 2, 15000, true },
                { 50, 2, 20000, true },
                { 50, 2, 25000, true },
                { 50, 2, 30000, true },
                { 50, 2, 35000, true },
                { 50, 2, 40000, true },
                { 50, 2, 45000, true },
                { 50, 2, 50000, true },
                { 50, 2, 100000, true },
                { 50, 2, 150000, true },
                { 50, 2, 200000, true },
        });
    }

    /* Warming up would bring data into hbase block cache. To run cold only benchmarks,
     * hbase block cache must be disabled first:
     * hfile.block.cache.size = 0
     */

    @Test
    public void indexLookupWarm() throws Exception {
        LOG.info("indexLookupWarm");
        runBenchmark(1, () -> benchmark(true, NUM_EXECS, true, onOlap));
    }

    @Test
    public void indexScanWarm() throws Exception {
        LOG.info("indexScanWarm");
        runBenchmark(1, () -> benchmark(false, NUM_EXECS, true, onOlap));
    }
}
