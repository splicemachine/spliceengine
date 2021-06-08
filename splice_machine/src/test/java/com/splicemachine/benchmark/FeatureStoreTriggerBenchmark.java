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
    private static final int DEFAULT_ENTITIES = 1 << 12;
    private static final int DEFAULT_OPS = 16;

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher("FeatureStoreTriggerBenchmark");

    private static final String schema   = System.getProperty("splice.benchmark.schema", spliceSchemaWatcher.schemaName);
    private static final int connections = Integer.getInteger("splice.benchmark.connections", DEFAULT_CONNECTIONS);
    private static final int entities  = Integer.getInteger("splice.benchmark.entities", DEFAULT_ENTITIES);
    private static final int updates  = Integer.getInteger("splice.benchmark.operations", DEFAULT_OPS);

    private static int numHosts;
    private static Connection testConnection;
    private static Statement testStatement;

    static Connection makeConnection() throws SQLException {
        return makeConnection(null);
    }

    static Connection makeConnection(Boolean useOLAP) throws SQLException {
        SpliceNetConnection.ConnectionBuilder builder = SpliceNetConnection.newBuilder();
        if (useOLAP != null) {
            builder.useOLAP(useOLAP);
        }
        Connection connection = builder.build();
        connection.setSchema(schema);
        connection.setAutoCommit(true);
        return connection;
    }

    @BeforeClass
    public static void setUp() throws Exception {
        LOG.info("Parameter: schema      = " + schema);
        LOG.info("Parameter: connections = " + connections);

        RegionServerInfo[] info = getRegionServerInfo();
        for (RegionServerInfo rs : info) {
            LOG.info(String.format("HOST: %s  SPLICE: %s (%s)", rs.hostName, rs.release, rs.buildHash));
        }

        numHosts = info.length;
        testConnection = makeConnection();
        testStatement = testConnection.createStatement();

        createTables();
    }

    private static void createTables() throws Exception {
        LOG.info("Creating tables...");

        // Pre-split FeatureTable
        String fileName = "/tmp/split_FeatureTable";
        testStatement.execute("DROP TABLE IF EXISTS SPLITKEYS");
        testStatement.execute("CREATE TABLE SPLITKEYS (pk int)");
        for (int split = 1; split < numHosts; ++split) {
            int pk = split * (entities / numHosts);
            testStatement.execute("INSERT INTO SPLITKEYS VALUES (" + pk + ")");
        }
        testStatement.execute(String.format("EXPORT('%s', false, null, null, null, null) SELECT * FROM SPLITKEYS", fileName));
        testStatement.execute(
            String.format("CREATE TABLE FeatureTable (" +
                "entity_key INTEGER PRIMARY KEY," +
                "last_update_ts TIMESTAMP," +
                "feature1 DOUBLE," +
                "feature2 DOUBLE)" +
                "LOGICAL SPLITKEYS LOCATION '%s'", fileName)
        );

        // Pre-split FeatureTableHistory
        fileName = "/tmp/split_FeatureTableHistory";
        testStatement.execute("DROP TABLE IF EXISTS SPLITKEYS");
        testStatement.execute("CREATE TABLE SPLITKEYS (pk1 int, pk2 timestamp)");
        String t0 = new Timestamp(0).toString();
        for (int split = 1; split < numHosts; ++split) {
            int pk = split * (entities / numHosts);
            testStatement.execute("INSERT INTO SPLITKEYS VALUES (" + pk + ",'" + t0 + "')");
        }
        testStatement.execute(String.format("EXPORT('%s', false, null, null, null, null) SELECT * FROM SPLITKEYS", fileName));
        testStatement.execute(
            String.format("CREATE TABLE FeatureTableHistory (" +
                "entity_key INTEGER," +
                "asof_ts TIMESTAMP," +
                "ingest_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP," +
                "feature1 DOUBLE," +
                "feature2 DOUBLE," +
                "PRIMARY KEY (entity_key, asof_ts))" +
                "LOGICAL SPLITKEYS LOCATION '%s'", fileName)
        );
    }

    static final String STAT_ERROR = "ERROR";
    static final String STAT_CREATE = "CREATE";
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
            conn.setAutoCommit(false);
            PreparedStatement insert = conn.prepareStatement("INSERT INTO FeatureStaging VALUES (?, ?, ?, ?, ?)");
            Random rnd = ThreadLocalRandom.current();
            int maxDay = (int)((ts1 - ts0 + DAYMS - 1) / DAYMS);
            int[] used = new int[maxDay];
            for (;;) {
                int entity = taskId.incrementAndGet();  // [1 .. entities]
                if (entity > entities) break;
                for (int update = 1; update <= updates; ++update) {
                    insert.setInt(1, entity);
                    insert.setInt(2, update);
                    // Pick unique day
                    int day;
                    do {
                        day = rnd.nextInt(maxDay);
                    } while (used[day] == entity);
                    used[day] = entity;
                    insert.setTimestamp(3, new Timestamp(ts0 + day * DAYMS));
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
            conn.commit();
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
            abort = true;
        }
    }

    private void populateStagingTable() throws SQLException {
        testStatement.execute("DROP TABLE IF EXISTS FeatureStaging");
        String fileName = "/tmp/split_FeatureStaging";
        testStatement.execute("DROP TABLE IF EXISTS SPLITKEYS");
        testStatement.execute("CREATE TABLE SPLITKEYS (pk1 int, pk2 int)");
        for (int split = 1; split < numHosts; ++split) {
            int pk = split * (entities / numHosts);
            testStatement.execute("INSERT INTO SPLITKEYS VALUES (" + pk + ",0)");
        }
        testStatement.execute(String.format("EXPORT('%s', false, null, null, null, null) SELECT * FROM SPLITKEYS", fileName));

        testStatement.execute(
                String.format("CREATE TABLE FeatureStaging(" +
                        "entity_key INTEGER," +
                        "update_order INTEGER," +
                        "ts TIMESTAMP," +
                        "feature1 DOUBLE," +
                        "feature2 DOUBLE," +
                        "PRIMARY KEY (entity_key, update_order))" +
                        "LOGICAL SPLITKEYS LOCATION '%s'", fileName)
        );

        long nextTimestamp = curTimestamp + updates * 2 * DAYMS;
        taskId.set(0);
        runBenchmark(connections, () -> populateStagingTable(curTimestamp, nextTimestamp));
        curTimestamp = nextTimestamp;
    }

    public void insertAndUpdateFromBatches() {
        try (Connection conn = makeConnection()) {
            try (PreparedStatement statement = conn.prepareStatement(
                    "INSERT INTO FeatureTable --splice-properties insertMode=UPSERT\n" +
                            "SELECT entity_key, ts, feature1, feature2 " +
                            "FROM FeatureStaging WHERE update_order=?")) {
                for (int update = 1; update <= updates; ++update) {
                    long start = System.currentTimeMillis();
                    statement.setInt(1, update);
                    int count = statement.executeUpdate();
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

    private void createFeatureTableTriggers() throws SQLException {
        testStatement.execute(
                "CREATE TRIGGER FeatureTable_INSERTS " +
                        "AFTER INSERT ON FeatureTable " +
                        "REFERENCING NEW_TABLE AS NEWW " +
                        "FOR EACH STATEMENT " +
                        "INSERT INTO FeatureTableHistory (entity_key, asof_ts, ingest_ts, feature1, feature2) --splice-properties insertMode=UPSERT\n" +
                        "SELECT NEWW.entity_key, NEWW.last_update_ts, CURRENT_TIMESTAMP, NEWW.feature1, NEWW.feature2 FROM NEWW"
        );
        testStatement.execute(
                "CREATE TRIGGER FeatureTable_UPDATES " +
                        "AFTER UPDATE ON FeatureTable " +
                        "REFERENCING NEW_TABLE AS NEWW " +
                        "FOR EACH STATEMENT " +
                        "INSERT INTO FeatureTableHistory ( entity_key, asof_ts, ingest_ts, feature1, feature2) --splice-properties insertMode=UPSERT\n" +
                        "SELECT NEWW.entity_key, NEWW.last_update_ts, CURRENT_TIMESTAMP, NEWW.feature1, NEWW.feature2 FROM NEWW"
        );
        testStatement.execute(
                "CREATE TRIGGER FeatureTableHistory_UPDATE_OOO " +
                        "AFTER UPDATE ON FeatureTable " +
                        "REFERENCING NEW_TABLE AS NEWW OLD_TABLE AS OLDW " +
                        "FOR EACH STATEMENT " +
                        "INSERT INTO FeatureTable (entity_key, last_update_ts, feature1, feature2) --splice-properties insertMode=UPSERT\n" +
                        "SELECT o.entity_key, o.last_update_ts, o.feature1, o.feature2 " +
                        "FROM OLDW o INNER JOIN NEWW n ON o.ENTITY_KEY=n.ENTITY_KEY AND o.LAST_UPDATE_TS > n.LAST_UPDATE_TS"
        );
    }

    private void dropFeatureTableTriggers() throws SQLException {
        testStatement.execute("DROP TRIGGER FeatureTable_INSERTS");
        testStatement.execute("DROP TRIGGER FeatureTable_UPDATES");
        testStatement.execute("DROP TRIGGER FeatureTableHistory_UPDATE_OOO");
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

    private void insertAndUpdateConcurrently(long ts0, long ts1) {
        int idx = lastIdx.getAndIncrement();
        PriorityQueue<Row> pq = new PriorityQueue<>((r1, r2) -> Long.signum(r1.delivery - r2.delivery));
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
    public void updateFeatureTable() throws SQLException {
        LOG.info("Updating the feature table from a pre-populated staging table");

        populateStagingTable();
        Assert.assertFalse(abort);

        try {
            createFeatureTableTriggers();

            long start = System.currentTimeMillis();
            runBenchmark(1, () -> insertAndUpdateFromBatches());
            long end = System.currentTimeMillis();
            Assert.assertFalse(abort);

            long throughput = getCountStats(STAT_UPDATE) * 1000L / (end - start);
            LOG.info(STAT_UPDATE + " throughput: " + throughput + " ops/sec");

        }
        finally {
            dropFeatureTableTriggers();
        }
    }

    @Test
    public void updateFeatureTableConcurrently() throws SQLException {
        LOG.info("Updating the feature table concurrently");

        try {
            createFeatureTableTriggers();

            long nextTimestamp = curTimestamp + updates * 2 * DAYMS;
            long start = System.currentTimeMillis();
            runBenchmark(connections, () -> insertAndUpdateConcurrently(curTimestamp, nextTimestamp));
            long end = System.currentTimeMillis();
            curTimestamp = nextTimestamp;
            Assert.assertFalse(abort);

            long throughput = getCountStats(STAT_UPDATE) * 1000L / (end - start);
            LOG.info(STAT_UPDATE + " throughput: " + throughput + " ops/sec");

        }
        finally {
            dropFeatureTableTriggers();
        }
    }

    private void createHistoryTableTriggers() throws SQLException {
        testStatement.execute(
                "CREATE TRIGGER FeatureTableHistory_INSERT " +
                        "AFTER INSERT ON FeatureTableHistory " +
                        "REFERENCING NEW_TABLE AS NEWW " +
                        "FOR EACH STATEMENT " +
                        "INSERT INTO FeatureTable (entity_key, last_update_ts, feature1, feature2) --splice-properties insertMode=UPSERT\n" +
                        "SELECT n.entity_key, n.asof_ts, n.feature1, n.feature2 " +
                        "FROM NEWW n LEFT JOIN FeatureTable o \n" +
                        "ON n.entity_key = o.entity_key WHERE (o.last_update_ts IS NULL OR o.last_update_ts < n.asof_ts)"
        );
    }

    private void dropHistoryTableTriggers() throws SQLException {
        testStatement.execute("DROP TRIGGER FeatureTableHistory_INSERT");
    }

    private void insertAndUpdateFromBatches2() {
        try (Connection conn = makeConnection()) {
            try (PreparedStatement statement = conn.prepareStatement(
                    "INSERT INTO FeatureTableHistory " +
                            "SELECT entity_key, ts, CURRENT_TIMESTAMP, feature1, feature2 " +
                            "FROM FeatureStaging WHERE update_order=?")) {
                for (int update = 1; update <= updates; ++update) {
                    long start = System.currentTimeMillis();
                    statement.setInt(1, update);
                    int count = statement.executeUpdate();
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

    static final AtomicInteger lastIdx2 = new AtomicInteger(0);

    private void insertAndUpdateConcurrently2(long ts0, long ts1) {
        int idx = lastIdx2.getAndIncrement();
        PriorityQueue<Row> pq = new PriorityQueue<>((r1, r2) -> Long.signum(r1.delivery - r2.delivery));
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
            try (PreparedStatement statement = conn.prepareStatement("INSERT INTO FeatureTableHistory (entity_key, asof_ts, feature1, feature2) VALUES(?,?,?,?)")) {
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
    public void updateHistoryTable() throws SQLException {
        LOG.info("Updating the history table from a pre-populated staging table");

        populateStagingTable();
        Assert.assertFalse(abort);

        try {
            createHistoryTableTriggers();

            long start = System.currentTimeMillis();
            runBenchmark(1, () -> insertAndUpdateFromBatches2());
            long end = System.currentTimeMillis();
            Assert.assertFalse(abort);

            long throughput = getCountStats(STAT_UPDATE) * 1000L / (end - start);
            LOG.info(STAT_UPDATE + " throughput: " + throughput + " ops/sec");

        }
        finally {
            dropHistoryTableTriggers();
        }
    }

    @Test
    public void updateHistoryTableConcurrently() throws SQLException {
        LOG.info("Updating the history table concurrently");

        try {
            createHistoryTableTriggers();

            long nextTimestamp = curTimestamp + updates * 2 * DAYMS;
            long start = System.currentTimeMillis();
            runBenchmark(connections, () -> insertAndUpdateConcurrently2(curTimestamp, nextTimestamp));
            long end = System.currentTimeMillis();
            curTimestamp = nextTimestamp;
            Assert.assertFalse(abort);

            long throughput = getCountStats(STAT_UPDATE) * 1000L / (end - start);
            LOG.info(STAT_UPDATE + " throughput: " + throughput + " ops/sec");

        }
        finally {
            dropHistoryTableTriggers();
        }
    }

}
