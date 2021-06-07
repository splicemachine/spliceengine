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
public class IndexPrefixIterationBenchmark extends Benchmark {

    private static final Logger LOG = Logger.getLogger(IndexPrefixIterationBenchmark.class);

    private static final String SCHEMA = IndexPrefixIterationBenchmark.class.getSimpleName();

    private static final String TABLE_NAME = "BASE_TABLE";
    private static final String IDX_NAME = "RIGHT_IDX";
    private static final int NUM_CONNECTIONS = 1;
    private static final int NUM_EXECS = 10;
    private static final int NUM_WARMUP_RUNS = 1;

    private final int tableSize;
    private final int batchSize;

    public IndexPrefixIterationBenchmark(int tableSize, int batchSize) {
        this.tableSize = tableSize;
        this.batchSize = batchSize;
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

        testStatement.execute("CREATE TABLE " + TABLE_NAME + " (col1_pk INTEGER, col2 INTEGER, col3 INTEGER, col4_pk INTEGER, col5 INTEGER,  PRIMARY KEY(col1_pk, col4_pk))");
        testStatement.execute("CREATE INDEX " + IDX_NAME + " on " + TABLE_NAME + " (col2, col3)");

        curSize.set(0);
        runBenchmark(NUM_CONNECTIONS, () -> populateTable(TABLE_NAME, tableSize));

        testStatement.execute(String.format("call syscs_util.syscs_flush_table('%s', '%s')", SCHEMA, TABLE_NAME));

        LOG.info("Collect statistics");
        try (ResultSet rs = testStatement.executeQuery("ANALYZE SCHEMA " + SCHEMA)) {
            assertTrue(rs.next());
        }

        testStatement.execute("set session_property favorIndexPrefixIteration=true");
    }

    @After
    public void tearDown() throws Exception {
        testStatement.execute("DROP TABLE " + TABLE_NAME);
        testStatement.close();
        testConnection.close();
    }

    static final String STAT_ERROR = "ERROR";
    static final String STAT_PREP = "PREPARE ";
    static AtomicInteger curSize = new AtomicInteger(0);

    private void populateTable(String tableName, int size) {
        int newSize = 0;
        try (Connection conn = makeConnection()) {
            try (PreparedStatement insert = conn.prepareStatement("INSERT INTO " + tableName + " VALUES (?,?,?,?,?)")) {
                newSize = curSize.addAndGet(batchSize);
                for (int i = 0; i < newSize; ++i) {
                    insert.setInt(1, newSize);
                    insert.setInt(2, newSize);
                    insert.setInt(3, i);
                    insert.setInt(4, i);
                    insert.setInt(5, i);
                    insert.addBatch();
                }
                long start = System.currentTimeMillis();
                int[] counts = insert.executeBatch();
                long end = System.currentTimeMillis();
                int count = 0;
                for (int c : counts) count += c;
                if (count != batchSize) {
                    updateStats(STAT_ERROR);
                }
                if (count > 0) {
                    updateStats(STAT_PREP + tableName, count, end - start);
                }
            }
            try (Statement insert = conn.createStatement()) {
                for (; ; ) {
                    if (newSize >= size) break;
                    String insertStmt =
                        String.format("INSERT INTO " + tableName + " select %s, %s, col3+%s, col4_pk+%s, col5+%s" +
                                      " from " + tableName + " --splice-properties useSpark=true\n", newSize, newSize, newSize, newSize, newSize);
                    newSize *= 2;
                    long start = System.currentTimeMillis();
                    int count = insert.executeUpdate(insertStmt);
                    long end = System.currentTimeMillis();
                    if (count > 0) {
                        updateStats(STAT_PREP + tableName, count, end - start);
                    }
                }
            }
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
        }
    }

    private int getRowCount(ResultSet rs) throws SQLException {
        assertTrue(rs.next());
        return rs.getInt(1);
    }

    private void benchmark(int numExec, boolean warmUp, boolean useIdx, boolean onOlap) {
        String sqlText = String.format("select count(*) from \n" +
                "   %s R --splice-properties index=%s, useSpark=%b\n",
                TABLE_NAME, useIdx ? IDX_NAME : "null", onOlap);
        String dataLabel = warmUp ? "Warm" : "Cold";
        if (useIdx) {
            dataLabel += "Idx";
            sqlText += " where R.col3 between 0 and 9";
        }
        else {
            dataLabel += "BaseTable";
            sqlText += " where R.col4_pk between 0 and 9";
        }
        if (onOlap)
            dataLabel += "Olap";
        else
            dataLabel += "Oltp";

        try (Connection conn = makeConnection()) {
            try (Statement statement = conn.createStatement()) {
                statement.execute("set session_property favorIndexPrefixIteration=true");
            }
            try (PreparedStatement query = conn.prepareStatement(sqlText)) {
                // validate plan
                testQueryContains(conn, "explain " + sqlText, "IndexPrefixIteratorMode");

                // warm-up runs
                if (warmUp) {
                    for (int i = 0; i < NUM_WARMUP_RUNS; ++i) {
                        query.executeQuery();
                    }
                }

                // measure and validate result row count
                for (int i = 0; i < numExec; ++i) {
                    long start = System.currentTimeMillis();
                    try (ResultSet rs = query.executeQuery()) {
                        if (getRowCount(rs) != 10) {
                            updateStats(STAT_ERROR);
                        } else {
                            long stop = System.currentTimeMillis();
                            updateStats(dataLabel, stop - start);
                        }
                    } catch (SQLException ex) {
                        LOG.error("ERROR execution " + i + " of join benchmark using " + dataLabel + ": " + ex.getMessage());
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
                { 10, 10},
                { 100, 10 },
                { 1000, 10 },
                { 1000, 100 },
                { 10000, 10 },
                { 10000, 1000 },
                { 100000, 100 },
                { 100000, 10000 },
                { 1000000, 1000 },
                { 1000000, 65000},
                { 10000000, 1000 },
                { 10000000, 65000 },
        });
    }

    /* Warming up would bring data into hbase block cache. To run cold only benchmarks,
     * hbase block cache must be disabled first:
     * hfile.block.cache.size = 0
     */

    @Test
    public void testVariations() throws Exception {
        LOG.info("IndexPrefixIterationCold");
        runBenchmark(1, () -> benchmark(NUM_EXECS, false, false, false));
        runBenchmark(1, () -> benchmark(NUM_EXECS, false, false, true));
        runBenchmark(1, () -> benchmark(NUM_EXECS, false, true, false));
        runBenchmark(1, () -> benchmark(NUM_EXECS, false, true, true));

        LOG.info("IndexPrefixIterationWarm");
        runBenchmark(1, () -> benchmark(NUM_EXECS, true, false, false));
        runBenchmark(1, () -> benchmark(NUM_EXECS, true, false, true));
        runBenchmark(1, () -> benchmark(NUM_EXECS, true, true, false));
        runBenchmark(1, () -> benchmark(NUM_EXECS, true, true, true));
    }
}
