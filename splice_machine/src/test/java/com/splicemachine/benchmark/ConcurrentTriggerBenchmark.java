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

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceSchemaWatcher;
import com.splicemachine.test.Benchmark;
import com.splicemachine.util.StatementUtils;
import org.junit.*;
import org.junit.experimental.categories.Category;

import java.sql.*;
import java.util.concurrent.atomic.*;

import static org.junit.Assert.assertEquals;

/**
 * This benchmark should take about a minute to run.
 * It tests the performance of multiple row triggers
 * on the same target table.  For processing a given
 * insert row, all defined row triggers are
 * executed concurrently.
 */

@Category(Benchmark.class)
public class ConcurrentTriggerBenchmark extends Benchmark {

    private static final Logger LOG = LogManager.getLogger(ConcurrentTriggerBenchmark.class);

    private static final String SCHEMA = ConcurrentTriggerBenchmark.class.getSimpleName();
    private static final String TRIGGER_TABLE = "T1_with_trigger";
    private static final String TEST_TABLE = "T2";
    private static final String NOTRIGGER_TABLE = "T1";
    private static final int DEFAULT_SIZE = 10000;
    private static final int DEFAULT_CONNECTIONS = 1;
    private static final int DEFAULT_OPS = 1;

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
        testStatement.execute("CREATE TABLE " + TRIGGER_TABLE + " (a int, b int)");
        testStatement.execute("CREATE TABLE " + NOTRIGGER_TABLE + " (a int, b int)");
        testStatement.execute("CREATE TABLE " + TEST_TABLE + " (a int, b int)");

        size = DEFAULT_SIZE;
        runBenchmark(DEFAULT_CONNECTIONS, ConcurrentTriggerBenchmark::populateTables);
        curSize.set(size);

        LOG.info("Analyze, create triggers");
        testStatement.execute("ANALYZE SCHEMA " + SCHEMA);
        String triggerNamePrefix = "TR";
        for (int i=1; i <= 10; i++) {
            String triggerName = triggerNamePrefix + i;
            String createTriggerStmt =
              String.format("CREATE TRIGGER %s AFTER INSERT ON " + TRIGGER_TABLE +
              " REFERENCING NEW AS N FOR EACH ROW WHEN" +
              " (NOT EXISTS (SELECT %s.A FROM %s WHERE %s.A = N.A)) SIGNAL SQLSTATE '85101'",
              triggerName, TEST_TABLE, TEST_TABLE, TEST_TABLE);

            testStatement.execute(createTriggerStmt);
        }
    }

    @AfterClass
    public static void tearDown() throws Exception {
        testStatement.close();
        testConnection.close();
    }

    static final String STAT_ERROR = "ERROR";
    static final String STAT_CREATE = "CREATE";
    static final String STAT_INSERT_T = "INSERT_T";

    static int size;
    static AtomicInteger curSize = new AtomicInteger(0);
    static final int batchSize = 1000;

    private static void populateTables() {
        try (Connection conn = makeConnection()) {

            PreparedStatement insert2 = conn.prepareStatement("INSERT INTO " + NOTRIGGER_TABLE + " VALUES (?, ?)");
            PreparedStatement insert1 = conn.prepareStatement("INSERT INTO " + TEST_TABLE + " VALUES (?, ?)");

            for (;;) {
                int mini = curSize.getAndAdd(batchSize);
                int maxi = Math.min(mini + batchSize, size);
                if (mini >= maxi) break;
                for (int i = mini; i < maxi; ++i) {
                    insert1.setInt(1, i);
                    insert1.setInt(2, i);
                    insert1.addBatch();

                    insert2.setInt(1, i);
                    insert2.setInt(2, i);
                    insert2.addBatch();
                }
                long start = System.currentTimeMillis();
                int[] counts = insert1.executeBatch();
                long end = System.currentTimeMillis();
                int count = 0;
                for (int c : counts) count += c;
                if (count != maxi - mini) {
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
                if (count != maxi - mini) {
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
            PreparedStatement insert1 = conn.prepareStatement("INSERT INTO " + TRIGGER_TABLE + " SELECT * from "+ NOTRIGGER_TABLE);

            for (int i = 0; i < operations; ++i) {
                try {
                    long start = System.currentTimeMillis();
                    if (insert1.executeUpdate() != size) {
                        updateStats(STAT_ERROR);
                    }
                    else {
                        updateStats(STAT_INSERT_T, System.currentTimeMillis() - start);
                    }
                }
                catch (SQLException ex) {
                    LOG.error("ERROR inserting into " + TRIGGER_TABLE);
                    updateStats(STAT_ERROR);
                }
            }

            insert1.close();
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
        }
    }

    @Test
    public void insertBenchmarkSingle() throws Exception {
        LOG.info("insertBenchmarkSingle");
        runBenchmark(1, () -> doInserts(DEFAULT_OPS));
    }

    @Test
    public void insertBenchmarkMulti() throws Exception {
        LOG.info("insertBenchmarkMulti");
        runBenchmark(DEFAULT_CONNECTIONS, () -> doInserts(2 * DEFAULT_OPS));
    }
}
