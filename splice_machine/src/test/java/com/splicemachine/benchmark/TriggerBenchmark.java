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

import org.apache.log4j.Logger;
import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.test.Benchmark;
import com.splicemachine.util.StatementUtils;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.sql.*;
import java.util.concurrent.atomic.*;

import static org.junit.Assert.assertEquals;

@Category(Benchmark.class)
public class TriggerBenchmark extends Benchmark {

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

        getInfo();

        LOG.info("Create tables");
        testConnection = makeConnection();
        testStatement = testConnection.createStatement();
        testStatement.execute("CREATE TABLE " + TRIGGER_TABLE + " (col1 INTEGER NOT NULL PRIMARY KEY, col2 INTEGER)");
        testStatement.execute("CREATE TABLE " + NOTRIGGER_TABLE + " (col1 INTEGER NOT NULL PRIMARY KEY, col2 INTEGER)");
        testStatement.execute("CREATE TABLE " + INSERT_AUDIT_TABLE + " (time TIMESTAMP, audit INTEGER NOT NULL)");
        testStatement.execute("CREATE TABLE " + UPDATE_AUDIT_TABLE + " (time TIMESTAMP, audit INTEGER NOT NULL)");

        size = DEFAULT_SIZE;
        runBenchmark(DEFAULT_CONNECTIONS, TriggerBenchmark::populateTables);
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

    static final String STAT_ERROR = "ERROR";
    static final String STAT_CREATE = "CREATE";
    static final String STAT_INSERT_T = "INSERT_T";
    static final String STAT_INSERT_NOT = "INSERT_NOT";
    static final String STAT_UPDATE_T = "UPDATE_T";
    static final String STAT_UPDATE_NOT = "UPDATE_NOT";

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
                int maxi = Math.min(newSize + batchSize, size);
                for (int i = newSize; i < maxi; ++i) {
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
        runBenchmark(1, () -> doInserts(DEFAULT_OPS));
        assertEquals(curSize.get() - size, StatementUtils.onlyLong(testStatement, "select count(*) from " + INSERT_AUDIT_TABLE));
    }

    @Ignore("DB-11530")
    @Test
    public void insertBenchmarkMulti() throws Exception {
        LOG.info("insertBenchmarkMulti");
        runBenchmark(DEFAULT_CONNECTIONS, () -> doInserts(2 * DEFAULT_OPS));
        assertEquals(curSize.get() - size, StatementUtils.onlyLong(testStatement, "select count(*) from " + INSERT_AUDIT_TABLE));
    }

    @Test
    public void updateBenchmarkSingle() throws Exception {
        LOG.info("updateBenchmarkSingle");
        runBenchmark(1, () -> doUpdates(DEFAULT_OPS));
        assertEquals(curUpdate.get(), StatementUtils.onlyLong(testStatement, "select count(*) from " + UPDATE_AUDIT_TABLE));
    }

    @Ignore("DB-11530")
    @Test
    public void updateBenchmarkMulti() throws Exception  {
        LOG.info("updateBenchmarkMulti");
        runBenchmark(DEFAULT_CONNECTIONS, () -> doUpdates(2 * DEFAULT_OPS));
        assertEquals(curUpdate.get(), StatementUtils.onlyLong(testStatement, "select count(*) from " + UPDATE_AUDIT_TABLE));
    }
}
