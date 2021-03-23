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
import java.util.Arrays;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertTrue;

@Category(Benchmark.class)
@RunWith(Parameterized.class)
public class IndexLookupBenchmark extends Benchmark {

    private static final Logger LOG = Logger.getLogger(IndexLookupBenchmark.class);

    private static final String SCHEMA = IndexLookupBenchmark.class.getSimpleName();
    private static final String BASE_TABLE = "BASE_TABLE";
    private static final String BASE_TABLE_IDX = "BASE_TABLE_IDX";
    private static final String SIDE_TABLE = "SIDE_TABLE";
    private static final int NUM_CONNECTIONS = 10;
    private static final int NUM_EXECS = 20;
    private static final int NUM_WARMUP_RUNS = 5;

    private final int tableSize;
    private final boolean onOlap;
    private final int batchSize;

    public IndexLookupBenchmark(int tableSize, boolean onOlap) {
        this.tableSize = tableSize;
        this.onOlap = onOlap;
        this.batchSize = Math.min(tableSize, 1000);
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
        testStatement.execute("CREATE TABLE " + BASE_TABLE + " (col1 INTEGER NOT NULL, col2 INTEGER, col3 INTEGER, col4 INTEGER, col5 INTEGER)");
        testStatement.execute("CREATE INDEX " + BASE_TABLE_IDX + " on " + BASE_TABLE + " (col1, col2)");

        testStatement.execute("CREATE TABLE " + SIDE_TABLE + " (col1 INTEGER NOT NULL, col2 INTEGER, col3 INTEGER, col4 INTEGER, col5 INTEGER)");

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
        testStatement.execute("DROP TABLE " + SIDE_TABLE);
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
            try (PreparedStatement insert1 = conn.prepareStatement("INSERT INTO " + BASE_TABLE + " VALUES (?,?,?,?,?)")) {
                for (; ; ) {
                    int newSize = curSize.getAndAdd(batchSize);
                    if (newSize >= size) break;
                    for (int i = newSize; i < newSize + batchSize; ++i) {
                        insert1.setInt(1, i);
                        insert1.setInt(2, i);
                        insert1.setInt(3, i);
                        insert1.setInt(4, i);
                        insert1.setInt(5, i);
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

    private static int getRowCount(ResultSet rs) throws SQLException {
        assertTrue(rs.next());
        return rs.getInt(1);
    }

    private void benchmark(boolean isIndexLookup, int numExec, boolean warmUp, boolean onOlap) {
        String sqlText;
        String dataLabel;
        if (isIndexLookup) {
            sqlText = String.format("select count(col3) from %s --splice-properties index=%s, useSpark=false", BASE_TABLE, BASE_TABLE_IDX);
            dataLabel = INDEX_LOOKUP;
        } else {
            sqlText = String.format("select count(col1) from %s --splice-properties index=%s, useSpark=false", BASE_TABLE, BASE_TABLE_IDX);
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
                    rowContainsQuery(new int[]{6}, "explain " + sqlText, conn, new String[]{"IndexLookup"});
                } else {
                    rowContainsQuery(new int[]{6}, "explain " + sqlText, conn, new String[]{"IndexScan"});
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
                { 1000, false },
                { 5000, false },
                { 10000, false },
                { 15000, false },
                { 20000, false },
                { 25000, false },
                { 30000, false },
                { 35000, false },
                { 40000, false },
                { 45000, false },
                { 50000, false },
                { 100000, false },
                { 150000, false },
                { 200000, false },
                { 1000, true },
                { 5000, true },
                { 10000, true },
                { 15000, true },
                { 20000, true },
                { 25000, true },
                { 30000, true },
                { 35000, true },
                { 40000, true },
                { 45000, true },
                { 50000, true },
                { 100000, true },
                { 150000, true },
                { 200000, true },
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
