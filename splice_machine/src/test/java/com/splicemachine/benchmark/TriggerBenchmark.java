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
import com.splicemachine.util.StatementUtils;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.*;

import static org.junit.Assert.assertEquals;

@Category(Benchmark.class)
public class TriggerBenchmark {

    private static final Logger LOG = Logger.getLogger(TriggerBenchmark.class);

    private static final String SCHEMA = TriggerBenchmark.class.getSimpleName();
    private static final String TRIGGER_TABLE = "TRIGGERS";
    private static final String NOTRIGGER_TABLE = "NOTRIGGERS";
    private static final String INSERT_AUDIT_TABLE = "INSERT_AUDIT";
    private static final String UPDATE_AUDIT_TABLE = "UPDATE_AUDIT";
    private static final int DEFAULT_SIZE = 100000;
    private static final int DEFAULT_CONNECTIONS = 10;
    private static final int DEFAULT_OPS = 10000;

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

    @BeforeClass
    public static void setUp() throws Exception {

        LOG.info("Clean up");
        try (Connection connection = SpliceNetConnection.getDefaultConnection()) {
            try (Statement statement = connection.createStatement()) {
                try {
                    statement.execute("DROP SCHEMA " + spliceSchemaWatcher.schemaName + " CASCADE");
                }
                catch (SQLException ex) {
                    if (!ex.getSQLState().equals("42Y07")) {    // ignore if schema doesn't exist
                        throw ex;
                    }
                }
                statement.execute("CALL SYSCS_UTIL.VACUUM()");
                statement.execute("CREATE SCHEMA " + spliceSchemaWatcher.schemaName);
            }
        }

        LOG.info("Create tables");
        testConnection = makeConnection();
        testStatement = testConnection.createStatement();
        testStatement.execute("CREATE TABLE " + TRIGGER_TABLE + " (col1 INTEGER NOT NULL PRIMARY KEY, col2 INTEGER)");
        testStatement.execute("CREATE TABLE " + NOTRIGGER_TABLE + " (col1 INTEGER NOT NULL PRIMARY KEY, col2 INTEGER)");
        testStatement.execute("CREATE TABLE " + INSERT_AUDIT_TABLE + " (time TIMESTAMP, audit INTEGER NOT NULL)");
        testStatement.execute("CREATE TABLE " + UPDATE_AUDIT_TABLE + " (time TIMESTAMP, audit INTEGER NOT NULL)");

        size = DEFAULT_SIZE;
        run(DEFAULT_CONNECTIONS, TriggerBenchmark::populateTables);
        curSize.set(size);

        LOG.info("Analyze, create triggers");
        testStatement.execute("ANALYZE SCHEMA " + SCHEMA);
        testStatement.execute("CREATE TRIGGER tinsert AFTER INSERT ON " + TRIGGER_TABLE +
                        " REFERENCING NEW AS n FOR EACH ROW INSERT INTO " + INSERT_AUDIT_TABLE +
                        " VALUES (CURRENT_TIMESTAMP, n.col1 + 1000000000)");
        testStatement.execute("CREATE TRIGGER tupdate AFTER UPDATE ON " + TRIGGER_TABLE + "" +
                        " REFERENCING NEW AS n FOR EACH ROW INSERT INTO " + UPDATE_AUDIT_TABLE +
                        " VALUES (CURRENT_TIMESTAMP, n.col1 + 1000000000)");
    }

    @AfterClass
    public static void tearDown() throws Exception {
        testStatement.close();
        testConnection.close();
    }

    // Statistics

    static final String STAT_ERROR = "ERROR";
    static final String STAT_CREATE = "CREATE";
    static final String STAT_INSERT_T = "INSERT_T";
    static final String STAT_INSERT_NOT = "INSERT_NOT";
    static final String STAT_UPDATE_T = "UPDATE_T";
    static final String STAT_UPDATE_NOT = "UPDATE_NOT";

    static final int MAXSTATS = 64;
    static AtomicLongArray statsCnt = new AtomicLongArray(MAXSTATS);
    static AtomicLongArray statsSum = new AtomicLongArray(MAXSTATS);
    static AtomicLong nextReport = new AtomicLong(0);
    static long deadLine = Long.MAX_VALUE;
    static ConcurrentHashMap<String, Integer> indexMap = new ConcurrentHashMap<>(MAXSTATS);
    static AtomicInteger lastIdx = new AtomicInteger(0);


    static void updateStats(String stat) {
        updateStats(stat, 1, 0);
    }

    static void updateStats(String stat, long delta) {
        updateStats(stat, 1, delta);
    }

    static void updateStats(String stat, long increment, long delta) {
        int idx = indexMap.computeIfAbsent(stat, key -> lastIdx.getAndIncrement());
        if (idx >= MAXSTATS) {
            throw new RuntimeException("ERROR: Too many keys: " + idx);
        }

        statsCnt.addAndGet(idx, increment);
        statsSum.addAndGet(idx, delta);

        long curTime = System.currentTimeMillis();
        long t = nextReport.get();
        if (curTime > t && nextReport.compareAndSet(t, (t == 0 ? curTime : t) + 60000) && t > 0) {
            reportStats();
            if (curTime > deadLine) {
                throw new RuntimeException("Deadline has been reached");
            }
        }
    }

    static void reportStats() {
        StringBuilder sb = new StringBuilder().append("Statistics:");
        for (Map.Entry<String, Integer> entry : indexMap.entrySet()) {
            int idx = entry.getValue();
            long count = statsCnt.getAndSet(idx, 0);
            long time  = statsSum.getAndSet(idx, 0);
            if (count != 0) {
                long avg = (time + count / 2) / count;
                sb.append("\n\t").append(entry.getKey());
                sb.append("\tcalls: ").append(count);
                sb.append("\ttime: ").append(time / 1000).append(" s");
                sb.append("\tavg: ").append(avg).append(" ms");
            }
        }
        LOG.info(sb.toString());
    }

    static void resetStats() {
        reportStats();
        nextReport.set(0);
    }

    static void run(int numThreads, Runnable runnable) {
        Thread[] threads = new Thread[numThreads];
        for (int i = 0; i < threads.length; ++i) {
            Thread thread = new Thread(runnable);
            threads[i] = thread;
        }
        for (Thread t : threads) {
            t.start();
        }

        try {
            for (Thread thread : threads) {
                thread.join();
            }
        }
        catch (InterruptedException ie) {
            ie.printStackTrace();
        }
        resetStats();
    }

    static int size;
    static AtomicInteger curSize = new AtomicInteger(0);
    static final int batchSize = 1000;

    private static void populateTables() {
        try (Connection conn = makeConnection()) {
            PreparedStatement insert1 = conn.prepareStatement("INSERT INTO " + TRIGGER_TABLE + " VALUES (?, 0)");
            PreparedStatement insert2 = conn.prepareStatement("INSERT INTO " + NOTRIGGER_TABLE + " VALUES (?, 0)");

            for (;;) {
                int newSize = curSize.getAndAdd(batchSize);
                if (newSize >= size) break;
                for (int i = newSize; i < newSize + batchSize; ++i) {
                    insert1.setInt(1, i);
                    insert1.addBatch();

                    insert2.setInt(1, i);
                    insert2.addBatch();
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

                start = System.currentTimeMillis();
                counts = insert2.executeBatch();
                end = System.currentTimeMillis();
                count = 0;
                for (int c : counts) count += c;
                if (count != batchSize) {
                    updateStats(STAT_ERROR);
                }
                if (count > 0) {
                    updateStats(STAT_CREATE, count, end - start);
                }
            }

            insert1.close();
            insert2.close();
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
        }
    }

    private static void doInserts(int operations) {
        try (Connection conn = makeConnection()) {
            PreparedStatement insert1 = conn.prepareStatement("INSERT INTO " + TRIGGER_TABLE + " VALUES (?, 0)");
            PreparedStatement insert2 = conn.prepareStatement("INSERT INTO " + NOTRIGGER_TABLE + " VALUES (?, 0)");

            for (int i = 0; i < operations; ++i) {
                int key = curSize.getAndIncrement();

                long start = System.currentTimeMillis();
                try {
                    insert1.setInt(1, key);
                    if (insert1.executeUpdate() != 1) {
                        updateStats(STAT_ERROR);
                    }
                    else {
                        updateStats(STAT_INSERT_T, System.currentTimeMillis() - start);
                    }
                }
                catch (SQLException ex) {
                    LOG.error("ERROR inserting " + key + " into " + TRIGGER_TABLE);
                    updateStats(STAT_ERROR);
                }

                start = System.currentTimeMillis();
                try {
                    insert2.setInt(1, key);
                    if (insert2.executeUpdate() != 1) {
                        updateStats(STAT_ERROR);
                    }
                    else {
                        updateStats(STAT_INSERT_NOT, System.currentTimeMillis() - start);
                    }
                }
                catch (SQLException ex) {
                    LOG.error("ERROR inserting " + key + " into " + NOTRIGGER_TABLE);
                    updateStats(STAT_ERROR);
                }
            }

            insert1.close();
            insert2.close();
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
        }
    }

    static AtomicInteger curUpdate = new AtomicInteger(0);

    private static void doUpdates(int operations) {
        try (Connection conn = makeConnection()) {
            PreparedStatement update1 = conn.prepareStatement("UPDATE " + TRIGGER_TABLE + " SET col2 = col2 + 1 WHERE col1 = ?");
            PreparedStatement update2 = conn.prepareStatement("UPDATE " + NOTRIGGER_TABLE + "  SET col2 = col2 + 1 WHERE col1 = ?");

            for (int i = 0; i < operations; ++i) {
                int key = curUpdate.getAndIncrement() % size;

                long start = System.currentTimeMillis();
                try {
                    update1.setInt(1, key);
                    if (update1.executeUpdate() != 1) {
                        updateStats(STAT_ERROR);
                    }
                    else {
                        updateStats(STAT_UPDATE_T, System.currentTimeMillis() - start);
                    }
                }
                catch (SQLException ex) {
                    LOG.error("ERROR updating " + key + " in " + TRIGGER_TABLE);
                    updateStats(STAT_ERROR);
                }

                start = System.currentTimeMillis();
                try {
                    update2.setInt(1, key);
                    if (update2.executeUpdate() != 1) {
                        updateStats(STAT_ERROR);
                    }
                    else {
                        updateStats(STAT_UPDATE_NOT, System.currentTimeMillis() - start);
                    }
                }
                catch (SQLException ex) {
                    LOG.error("ERROR updating " + key + " in " + NOTRIGGER_TABLE);
                    updateStats(STAT_ERROR);
                }
            }

            update1.close();
            update2.close();
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
        }
    }

    @Test
    public void insertBenchmarkSingle() throws Exception {
        LOG.info("insertBenchmarkSingle");
        run(1, () -> doInserts(DEFAULT_OPS));
        assertEquals(curSize.get() - size, StatementUtils.onlyLong(testStatement, "select count(*) from " + INSERT_AUDIT_TABLE));
    }

    @Ignore("DB-11530")
    @Test
    public void insertBenchmarkMulti() throws Exception {
        LOG.info("insertBenchmarkMulti");
        run(DEFAULT_CONNECTIONS, () -> doInserts(2 * DEFAULT_OPS));
        assertEquals(curSize.get() - size, StatementUtils.onlyLong(testStatement, "select count(*) from " + INSERT_AUDIT_TABLE));
    }

    @Test
    public void updateBenchmarkSingle() throws Exception {
        LOG.info("updateBenchmarkSingle");
        run(1, () -> doUpdates(DEFAULT_OPS));
        assertEquals(curUpdate.get(), StatementUtils.onlyLong(testStatement, "select count(*) from " + UPDATE_AUDIT_TABLE));
    }

    @Ignore("DB-11530")
    @Test
    public void updateBenchmarkMulti() throws Exception  {
        LOG.info("updateBenchmarkMulti");
        run(DEFAULT_CONNECTIONS, () -> doUpdates(2 * DEFAULT_OPS));
        assertEquals(curUpdate.get(), StatementUtils.onlyLong(testStatement, "select count(*) from " + UPDATE_AUDIT_TABLE));
    }
}
