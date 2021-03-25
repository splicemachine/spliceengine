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
public class JoinBenchmark extends Benchmark {

    private static final Logger LOG = Logger.getLogger(JoinBenchmark.class);

    private static final String SCHEMA = JoinBenchmark.class.getSimpleName();
    private static final String LEFT_TABLE = "LEFT_TABLE";
    private static final String LEFT_IDX = "LEFT_IDX";
    private static final String RIGHT_TABLE = "RIGHT_TABLE";
    private static final String RIGHT_IDX = "RIGHT_IDX";
    private static final int NUM_CONNECTIONS = 10;
    private static final int NUM_EXECS = 20;
    private static final int NUM_WARMUP_RUNS = 5;

    private static final String NESTED_LOOP = "NestedLoop";
    private static final String BROADCAST = "Broadcast";
    private static final String MERGE = "Merge";
    private static final String MERGE_SORT = "SortMerge";
    private static final String CROSS = "Cross";

    private final int leftSize;
    private final int rightSize;
    private final boolean onOlap;
    private final int batchSize;

    public JoinBenchmark(int leftSize, int rightSize, boolean onOlap) {
        this.leftSize = leftSize;
        this.rightSize = rightSize;
        this.onOlap = onOlap;
        this.batchSize = Math.min(Math.min(leftSize, rightSize), 1000);
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
        testStatement.execute("CREATE TABLE " + LEFT_TABLE + " (col1 INTEGER, col2 INTEGER, col3 INTEGER, col4 INTEGER, col5 INTEGER)");
        testStatement.execute("CREATE INDEX " + LEFT_IDX + " on " + LEFT_TABLE + " (col2, col3)");

        testStatement.execute("CREATE TABLE " + RIGHT_TABLE + " (col1_pk INTEGER PRIMARY KEY, col2 INTEGER, col3 INTEGER, col4 INTEGER, col5 INTEGER)");
        testStatement.execute("CREATE INDEX " + RIGHT_IDX + " on " + RIGHT_TABLE + " (col2, col3)");

        curSize.set(0);
        runBenchmark(NUM_CONNECTIONS, () -> populateTable(LEFT_TABLE, leftSize));

        curSize.set(0);
        runBenchmark(NUM_CONNECTIONS, () -> populateTable(RIGHT_TABLE, rightSize));

        testStatement.execute(String.format("call syscs_util.syscs_flush_table('%s', '%s')", SCHEMA, LEFT_TABLE));
        testStatement.execute(String.format("call syscs_util.syscs_flush_table('%s', '%s')", SCHEMA, RIGHT_TABLE));

        LOG.info("Collect statistics");
        try (ResultSet rs = testStatement.executeQuery("ANALYZE SCHEMA " + SCHEMA)) {
            assertTrue(rs.next());
        }
    }

    @After
    public void tearDown() throws Exception {
        testStatement.execute("DROP TABLE " + LEFT_TABLE);
        testStatement.execute("DROP TABLE " + RIGHT_TABLE);
        testStatement.close();
        testConnection.close();
    }

    static final String STAT_ERROR = "ERROR";
    static final String STAT_PREP = "PREPARE ";
    static final String KEY_JOIN = " JOIN ON KEY";
    static AtomicInteger curSize = new AtomicInteger(0);

    private void populateTable(String tableName, int size) {
        try (Connection conn = makeConnection()) {
            try (PreparedStatement insert = conn.prepareStatement("INSERT INTO " + tableName + " VALUES (?,?,?,?,?)")) {
                for (; ; ) {
                    int newSize = curSize.getAndAdd(batchSize);
                    if (newSize >= size) break;
                    for (int i = newSize; i < newSize + batchSize; ++i) {
                        insert.setInt(1, i);
                        insert.setInt(2, i);
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

    private void benchmark(String joinStrategy, boolean isKeyJoin, int numExec, boolean warmUp, boolean onOlap) {
        boolean isMergeJoin = joinStrategy.equals(MERGE);
        String sqlText = String.format("select count(L.col2) from --splice-properties joinOrder=fixed\n" +
                "   %s L --splice-properties index=%s, useSpark=%b\n" +
                " , %s R --splice-properties joinStrategy=%s, index=%s\n",
                LEFT_TABLE, isKeyJoin ? LEFT_IDX : "null",
                onOlap, RIGHT_TABLE, joinStrategy,
                isKeyJoin ? "null" : (isMergeJoin ? RIGHT_IDX : "null"));
        String dataLabel;
        if (isKeyJoin) {
            sqlText += " where L.col2 = R.col1_pk";
            dataLabel = joinStrategy + KEY_JOIN;
        } else {
            sqlText += " where L.col4 = R.col4";
            dataLabel = joinStrategy;
        }
        try (Connection conn = makeConnection()) {
            try (PreparedStatement query = conn.prepareStatement(sqlText)) {
                // validate plan
                int[] expectedRows = {6,6,8};
                String[] row6 = {joinStrategy + "Join"};
                if (joinStrategy.equals(MERGE_SORT)) {
                    row6[0] = "MergeSortJoin";
                }
                String[] row6or7;
                String[] row8;
                if (isKeyJoin) {
                    row6or7 = new String[]{"preds=[(L.COL2[4:1] = R.COL1_PK[4:2])]"};
                    row8 = new String[]{"IndexScan[LEFT_IDX"};
                    if (joinStrategy.equals(NESTED_LOOP)) {
                        expectedRows[1] = 7;
                        row6or7[0] = "preds=[(L.COL2[1:1] = R.COL1_PK[2:1])]";
                    }
                } else {
                    row6or7 = new String[]{"preds=[(L.COL4[4:2] = R.COL4[4:3])]"};
                    row8 = new String[]{"TableScan[LEFT_TABLE"};
                    if (joinStrategy.equals(NESTED_LOOP)) {
                        expectedRows[1] = 7;
                        row6or7[0] = "preds=[(L.COL4[1:2] = R.COL4[2:1])]";
                    }
                }
                rowContainsQuery(expectedRows, "explain " + sqlText, conn, row6, row6or7, row8);

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
                        if (getRowCount(rs) != Math.min(leftSize, rightSize)) {
                            updateStats(STAT_ERROR);
                        } else {
                            long stop = System.currentTimeMillis();
                            updateStats(dataLabel, stop - start);
                        }
                    } catch (SQLException ex) {
                        LOG.error("ERROR execution " + i + " of join benchmark using " + joinStrategy + ": " + ex.getMessage());
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
                { 10, 10, false },
                { 100, 10, false },
                { 1000, 10, false },
                { 10000, 10, false },
                { 100000, 10, false },
                { 1000000, 10, false },
                { 10, 100, false },
                { 100, 100, false },
                { 1000, 100, false },
                { 10000, 100, false },
                { 100000, 100, false },
                { 1000000, 100, false },
                { 10, 1000, false },
                { 100, 1000, false },
                { 1000, 1000, false },
                { 10000, 1000, false },
                { 100000, 1000, false },
                { 1000000, 1000, false },
                { 10, 10000, false },
                { 100, 10000, false },
                { 1000, 10000, false },
                { 10000, 10000, false },
                { 100000, 10000, false },
                { 1000000, 10000, false },
                { 10, 100000, false },
                { 100, 100000, false },
                { 1000, 100000, false },
                { 10000, 100000, false },
                { 10, 1000000, false },
                { 100, 1000000, false },
                { 1000, 1000000, false },
                { 10000, 1000000, false },
                { 10, 10, true },
                { 100, 10, true },
                { 1000, 10, true },
                { 10000, 10, true },
                { 100000, 10, true },
                { 1000000, 10, true },
                { 10, 100, true },
                { 100, 100, true },
                { 1000, 100, true },
                { 10000, 100, true },
                { 100000, 100, true },
                { 1000000, 100, true },
                { 10, 1000, true },
                { 100, 1000, true },
                { 1000, 1000, true },
                { 10000, 1000, true },
                { 100000, 1000, true },
                { 1000000, 1000, true },
                { 10, 10000, true },
                { 100, 10000, true },
                { 1000, 10000, true },
                { 10000, 10000, true },
                { 100000, 10000, true },
                { 1000000, 10000, true },
                { 10, 100000, true },
                { 100, 100000, true },
                { 1000, 100000, true },
                { 10000, 100000, true },
                { 10, 1000000, true },
                { 100, 1000000, true },
                { 1000, 1000000, true },
                { 10000, 1000000, true },
        });
    }

    /* Warming up would bring data into hbase block cache. To run cold only benchmarks,
     * hbase block cache must be disabled first:
     * hfile.block.cache.size = 0
     */

    @Test
    public void KeyJoinWarm() throws Exception {
        LOG.info("KeyJoinWarm");
        runBenchmark(1, () -> benchmark(NESTED_LOOP, true, NUM_EXECS, true, onOlap));
        runBenchmark(1, () -> benchmark(MERGE_SORT, true, NUM_EXECS, true, onOlap));
        runBenchmark(1, () -> benchmark(BROADCAST, true, NUM_EXECS, true, onOlap));
        runBenchmark(1, () -> benchmark(MERGE, true, NUM_EXECS, true, onOlap));
        if (onOlap) {
            runBenchmark(1, () -> benchmark(CROSS, true, NUM_EXECS, true, onOlap));
        }
    }

    @Test
    public void NonKeyJoinWarm() throws Exception {
        LOG.info("NonKeyJoinWarm");
        runBenchmark(1, () -> benchmark(NESTED_LOOP, false, NUM_EXECS, true, onOlap));
        runBenchmark(1, () -> benchmark(MERGE_SORT, false, NUM_EXECS, true, onOlap));
        runBenchmark(1, () -> benchmark(BROADCAST, false, NUM_EXECS, true, onOlap));
        // merge join is not feasible in this case
        if (onOlap) {
            runBenchmark(1, () -> benchmark(CROSS, false, NUM_EXECS, true, onOlap));
        }
    }
}
