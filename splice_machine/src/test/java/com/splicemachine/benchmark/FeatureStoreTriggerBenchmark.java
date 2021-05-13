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
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

@Category(Benchmark.class)
public class FeatureStoreTriggerBenchmark extends Benchmark {

    private static final Logger LOG = Logger.getLogger(FeatureStoreTriggerBenchmark.class);
    private static final int DEFAULT_CONNECTIONS = 10;

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher("FeatureStoreTriggerBenchmark");

    private static final String schema   = System.getProperty("splice.benchmark.schema", spliceSchemaWatcher.schemaName);
    private static final int connections = Integer.getInteger("splice.benchmark.connections", DEFAULT_CONNECTIONS);

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
                        "AFTER UPDATE ON FeatureTable \n" +
                        "REFERENCING NEW_TABLE AS NEWW OLD_TABLE AS OLDW\n" +
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
        runBenchmark(connections, () -> populateTable());
    }

    static final String STAT_ERROR = "ERROR";
    static final String STAT_CREATE = "CREATE";
    static final String STAT_INSERT = "INSERT";
    static final String STAT_UPDATE = "UPDATE";

    static final AtomicInteger taskId = new AtomicInteger(0);
    static final int entities = 1 << 17;
    static final int updates = 16;
    static boolean abort = false;

    private static void populateTable() {
        try (Connection conn = makeConnection()) {
            PreparedStatement insert = conn.prepareStatement("INSERT INTO FeatureStaging VALUES (?, ?, ?, ?, ?)");
            Random rnd = ThreadLocalRandom.current();
            long ts0 = Timestamp.valueOf("2000-01-01 00:00:00").getTime();
            for (;;) {
                int entity = taskId.incrementAndGet();  // [1 .. entities]
                if (entity > entities) break;
                for (int update = 1; update <= updates; ++update) {
                    insert.setInt(1, entity);
                    insert.setInt(2, update);
                    insert.setTimestamp(3, new Timestamp(ts0 + (rnd.nextInt(88) - 44) * 86400000L));
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

    @Before
    public void reset() {
        abort = false;
    }

    public void insertAndUpdateFromBatches() {
        try (Connection conn = makeConnection()) {
            try (Statement statement = conn.createStatement()) {
                long start = System.currentTimeMillis();
                int count = statement.executeUpdate(
                    "INSERT INTO FeatureTable " +
                        "SELECT Entity_Key, ts, feature1, feature2 " +
                        "FROM FeatureStaging WHERE update_order=1"
                );
                long end = System.currentTimeMillis();
                updateStats(STAT_INSERT, count, end - start);

                for (int update = 2; update <= updates; ++update) {
                    start = System.currentTimeMillis();
                    count = statement.executeUpdate(
                        String.format("UPDATE FeatureTable target " +
                            "SET (feature1, feature2, last_update_ts) = " +
                            "(SELECT feature1, feature2, ts " +
                            " FROM FeatureStaging s " +
                            " WHERE update_order=%d AND target.entity_key=s.entity_key)", update)
                    );
                    end = System.currentTimeMillis();
                    updateStats(STAT_UPDATE, count, end - start);
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
        runBenchmark(1, () -> insertAndUpdateFromBatches());
        Assert.assertFalse(abort);

        long count = getCountStats(STAT_INSERT);
        long time = getSumStats(STAT_INSERT);
        long throughput = time == 0 ? 0 : count * 1000L / time;
        LOG.info("INSERT throughput: " + throughput + " ops/sec");

        count = getCountStats(STAT_UPDATE);
        time = getSumStats(STAT_UPDATE);
        throughput = time == 0 ? 0 : count * 1000L / time;
        LOG.info("UPDATE throughput: " + throughput + " ops/sec");
    }
}
