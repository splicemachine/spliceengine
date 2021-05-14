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
 *
 */

package com.splicemachine.benchmark;

import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.test.Benchmark;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.sql.*;
import java.util.PriorityQueue;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

@Category(Benchmark.class)
public class FeatureStoreTriggerBenchmark extends Benchmark {

    private static final Logger LOG = Logger.getLogger(FeatureStoreTriggerBenchmark.class);
    private static final int DEFAULT_CONNECTIONS = 10;
    private static final int DEFAULT_ENTITIES = 1 << 17;
    private static final int DEFAULT_OPS = 16;

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher("FeatureStoreTriggerBenchmark");

    private static final String schema   = System.getProperty("splice.benchmark.schema", spliceSchemaWatcher.schemaName);
    private static final int connections = Integer.getInteger("splice.benchmark.connections", DEFAULT_CONNECTIONS);
    private static final int entities  = Integer.getInteger("splice.benchmark.entities", DEFAULT_ENTITIES);
    private static final int updates  = Integer.getInteger("splice.benchmark.operations", DEFAULT_OPS);

    static Connection makeConnection() throws SQLException {
        Connection connection = SpliceNetConnection.getDefaultConnection();
        connection.setSchema(schema);
        connection.setAutoCommit(true);
        return connection;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        LOG.info("Parameter: schema      = " + schema);
        LOG.info("Parameter: connections = " + connections);
        createTables();
    }

    private static void createTables() throws Exception {
        try (Connection conn = makeConnection()) {
            try (Statement statement = conn.createStatement()) {
                statement.execute(
                    "CREATE TABLE FeatureTable (" +
                        "entity_key INTEGER PRIMARY KEY," +
                        "last_update_ts TIMESTAMP," +
                        "feature1 DOUBLE," +
                        "feature2 DOUBLE)"
                );
                statement.execute(
                    "CREATE TABLE FeatureTableHistory (" +
                        "entity_key INTEGER," +
                        "asof_ts TIMESTAMP," +
                        "ingest_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                        "feature1 DOUBLE," +
                        "feature2 DOUBLE," +
                        "PRIMARY KEY ( entity_key, asof_ts))"
                );
                statement.execute(
                    "CREATE TRIGGER FeatureTable_INSERTS " +
                        "AFTER INSERT ON FeatureTable " +
                        "REFERENCING NEW_TABLE AS NEWW " +
                        "FOR EACH STATEMENT " +
                        "INSERT INTO FeatureTableHistory (entity_key, asof_ts, ingest_ts, feature1, feature2) --splice-properties insertMode=UPSERT\n" +
                        "SELECT NEWW.entity_key, NEWW.last_update_ts, CURRENT_TIMESTAMP, NEWW.feature1, NEWW.feature2 FROM NEWW"
                );
                statement.execute(
                    "CREATE TRIGGER FeatureTable_UPDATES " +
                        "AFTER UPDATE ON FeatureTable " +
                        "REFERENCING NEW_TABLE AS NEWW " +
                        "FOR EACH STATEMENT " +
                        "INSERT INTO FeatureTableHistory ( entity_key, asof_ts, ingest_ts, feature1, feature2) --splice-properties insertMode=UPSERT\n" +
                        "SELECT NEWW.entity_key, NEWW.last_update_ts, CURRENT_TIMESTAMP, NEWW.feature1, NEWW.feature2 FROM NEWW"
                );
                statement.execute(
                    "CREATE TRIGGER FeatureTableHistory_UPDATE_OOO " +
                        "AFTER UPDATE ON FeatureTable " +
                        "REFERENCING NEW_TABLE AS NEWW OLD_TABLE AS OLDW " +
                        "FOR EACH STATEMENT " +
                        "INSERT INTO FeatureTable (entity_key, last_update_ts, feature1, feature2) --splice-properties insertMode=UPSERT\n" +
                        "SELECT o.entity_key, o.last_update_ts, o.feature1, o.feature2 " +
                        "FROM OLDW o INNER JOIN NEWW n ON o.ENTITY_KEY=n.ENTITY_KEY AND o.LAST_UPDATE_TS > n.LAST_UPDATE_TS"
                );
                statement.execute(
                    "CREATE TABLE FeatureStaging(" +
                        "entity_key INTEGER," +
                        "update_order INTEGER," +
                        "ts TIMESTAMP," +
                        "feature1 DOUBLE," +
                        "feature2 DOUBLE," +
                        "PRIMARY KEY (entity_key, update_order))"
                );

            }
        }
    }

    static final String STAT_ERROR = "ERROR";
    static final String STAT_CREATE = "CREATE";
    static final String STAT_INSERT = "INSERT";
    static final String STAT_UPDATE = "UPDATE";
    static final long DAYMS = 86400000L;

    static final AtomicInteger taskId = new AtomicInteger(0);
    static long curTimestamp = Timestamp.valueOf("1999-12-01 00:00:00").getTime();
    static boolean abort = false;

    @Before
    public void reset() {
        abort = false;
    }

    // Generate timestamps between ts0 inclusive and ts1 exclusive
    private static void populateStagingTable(long ts0, long ts1) {
        try (Connection conn = makeConnection()) {
            PreparedStatement insert = conn.prepareStatement("INSERT INTO FeatureStaging VALUES (?, ?, ?, ?, ?)");
            Random rnd = ThreadLocalRandom.current();
            int maxDay = (int)((ts1 - ts0 + DAYMS - 1) / DAYMS);
            for (;;) {
                int entity = taskId.incrementAndGet();  // [1 .. entities]
                if (entity > entities) break;
                for (int update = 1; update <= updates; ++update) {
                    insert.setInt(1, entity);
                    insert.setInt(2, update);
                    insert.setTimestamp(3, new Timestamp(ts0 + rnd.nextInt(maxDay) * DAYMS));
                    insert.setDouble(4, rnd.nextDouble());
                    insert.setDouble(5, rnd.nextDouble());
                    insert.addBatch();
                }
                long start = System.currentTimeMillis();
                int[] counts = insert.executeBatch();
                insert.clearBatch();
                long end = System.currentTimeMillis();
                int count = 0;
                for (int c : counts) count += c;
                if (count != updates) {
                    updateStats(STAT_ERROR);
                }
                if (count > 0) {
                    updateStats(STAT_CREATE, count, end - start);
                }
            }
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
            abort = true;
        }
    }

    public void insertAndUpdateFromBatches() {
        try (Connection conn = makeConnection()) {
            try (Statement statement = conn.createStatement()) {
                for (int update = 1; update <= updates; ++update) {
                    long start = System.currentTimeMillis();
                    int count = statement.executeUpdate(
                        String.format("INSERT INTO FeatureTable --splice-properties insertMode=UPSERT\n" +
                                        "SELECT entity_key, ts, feature1, feature2 " +
                                        "FROM FeatureStaging WHERE update_order=%d", update)
                    );
                    long end = System.currentTimeMillis();
                    updateStats(STAT_UPDATE, count, end - start);
                }
            }
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
            abort = true;
        }
    }

    static class Row {
        int entity;
        long lastUpdate;
        double feature1;
        double feature2;
        long delivery;
        Row(int entity, long lastUpdate, double feature1, double feature2, long delivery) {
            this.entity = entity;
            this.lastUpdate = lastUpdate;
            this.feature1 = feature1;
            this.feature2 = feature2;
            this.delivery = delivery;
        }
    }

    static final AtomicInteger lastIdx = new AtomicInteger(0);

    public void insertAndUpdateConcurrently(long ts0, long ts1) {
        int idx = lastIdx.getAndIncrement();
        PriorityQueue<Row> pq = new PriorityQueue<Row>((r1, r2) -> Long.signum(r1.delivery - r2.delivery));
        Random rnd = ThreadLocalRandom.current();
        int minEntity = idx * entities / connections;
        int maxEntity = (idx + 1) * entities / connections;
        for (int update = 0; update < updates; ++update) {
            long updateTS = ts0 + update * (ts1 - ts0) / updates;
            for (int entity = minEntity; entity < maxEntity; ++entity) {
                Row row = new Row(entity, updateTS, rnd.nextDouble(), rnd.nextDouble(), rnd.nextLong() % (ts1 - ts0));
                pq.add(row);
            }
        }

        try (Connection conn = makeConnection()) {
            try (PreparedStatement statement = conn.prepareStatement("INSERT INTO FeatureTable --splice-properties insertMode=UPSERT\n VALUES(?,?,?,?)")) {
                for (;;) {
                    Row row = pq.poll();
                    if (row == null) break;

                    statement.setInt(1, row.entity);
                    statement.setTimestamp(2, new Timestamp(row.lastUpdate));
                    statement.setDouble(3, row.feature1);
                    statement.setDouble(4, row.feature2);
                    long start = System.currentTimeMillis();
                    int count = statement.executeUpdate();
                    long end = System.currentTimeMillis();
                    if (count != 1) {
                        updateStats(STAT_ERROR);
                    } else {
                        updateStats(STAT_UPDATE, end - start);
                    }
                }
            }
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
            abort = true;
        }
    }

    @Test
    public void benchmark() {
        LOG.info("Update the feature table from a pre-populated staging table");
        long nextTimestamp = curTimestamp + 60 * DAYMS;
        runBenchmark(connections, () -> populateStagingTable(curTimestamp, nextTimestamp));
        curTimestamp = nextTimestamp;
        Assert.assertFalse(abort);

        long start = System.currentTimeMillis();
        runBenchmark(1, () -> insertAndUpdateFromBatches());
        long end = System.currentTimeMillis();
        Assert.assertFalse(abort);

        long throughput = getCountStats(STAT_UPDATE) * 1000L / (end - start);
        LOG.info(STAT_UPDATE + " throughput: " + throughput + " ops/sec");
    }

    @Test
    public void benchmark2() {
        LOG.info("Update the feature table concurrently");
        long nextTimestamp = curTimestamp + 60 * DAYMS;
        long start = System.currentTimeMillis();
        runBenchmark(connections, () -> insertAndUpdateConcurrently(curTimestamp, nextTimestamp));
        long end = System.currentTimeMillis();
        curTimestamp = nextTimestamp;
        Assert.assertFalse(abort);

        long throughput = getCountStats(STAT_UPDATE) * 1000L / (end - start);
        LOG.info(STAT_UPDATE + " throughput: " + throughput + " ops/sec");
    }

}
