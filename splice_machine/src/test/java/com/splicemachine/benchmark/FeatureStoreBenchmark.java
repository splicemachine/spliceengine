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
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.*;
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;

@Category(Benchmark.class)
@RunWith(Parameterized.class)
public class FeatureStoreBenchmark extends Benchmark {

    private static final Logger LOG = Logger.getLogger(FeatureStoreBenchmark.class);
    private static final int DEFAULT_CONNECTIONS = 10;
    private static final int DEFAULT_OPS = 10000;
    private static final int DEFAULT_NROWS = 50000000;

    @ClassRule
    public static SpliceSchemaWatcher spliceSchemaWatcher = new SpliceSchemaWatcher("FeatureStoreBenchmark");

    private static final String schema   = System.getProperty("splice.benchmark.schema", spliceSchemaWatcher.schemaName);
    private static final int connections = Integer.getInteger("splice.benchmark.connections", DEFAULT_CONNECTIONS);
    private static final int operations  = Integer.getInteger("splice.benchmark.operations", DEFAULT_OPS);
    private static final int nrows       = Integer.getInteger("splice.benchmark.nrows", DEFAULT_NROWS);
    private static final boolean create  = System.getProperty("splice.benchmark.preloaded") == null;

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
        LOG.info("Parameter: operations  = " + operations);
        LOG.info("Parameter: #rows       = " + nrows);

        RegionServerInfo[] info = getRegionServerInfo();
        for (RegionServerInfo rs : info) {
            LOG.info(String.format("HOST: %s  SPLICE: %s (%s)", rs.hostName, rs.release, rs.buildHash));
        }

        if (create) {
            createSchema();
            createTables(info.length);

            try (Connection testConnection = makeConnection()) {
                try (Statement testStatement = testConnection.createStatement()) {
                    LOG.info("Major compaction...");
                    testStatement.execute(String.format("CALL SYSCS_UTIL.SYSCS_PERFORM_MAJOR_COMPACTION_ON_SCHEMA('%s')", schema));
                    LOG.info("Analyze schema...");
                    testStatement.execute(String.format("ANALYZE SCHEMA %s", schema));
                }
            }
        }
    }

    /* This is needed when schema is redefined */
    private static void createSchema() throws Exception {
        try (Connection connection = SpliceNetConnection.getDefaultConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute(String.format("CREATE SCHEMA %s", schema));
            }
        }
        catch (SQLException ex) {
            if (!ex.getSQLState().equals("X0Y68")) {
                throw ex;
            }
        }
    }

    static class TableConfig {
        String  tableName;
        int     numPK;
        int     numCols;
        long    numRows;

        TableConfig(String tableName, int numPK, int numCols, long numRows) {
            this.tableName = tableName;
            this.numPK = numPK;
            this.numCols = numCols;
            this.numRows = numRows;
        }
    }

    private static TableConfig T1 = new TableConfig("T1",   1,  10,     nrows);
    private static TableConfig T2 = new TableConfig("T2",   1,  100,    nrows);
    private static TableConfig T3 = new TableConfig("T3",   1,  1000,   nrows);
    private static TableConfig T4 = new TableConfig("T4",   2,  10,     nrows);
    private static TableConfig T5 = new TableConfig("T5",   2,  100,    nrows);
    private static TableConfig T6 = new TableConfig("T6",   2,  1000,   nrows);
    private static TableConfig T7 = new TableConfig("T7",   3,  10,     nrows);
    private static TableConfig T8 = new TableConfig("T8",   3,  100,    nrows);
    private static TableConfig T9 = new TableConfig("T9",   3,  1000,   nrows);

    private static TableConfig[] tableConfigs = { T1, T2, T3, T4, T5, T6, T7, T8, T9 };

    /*
     *    Tables are created according to the table configurations in <tableConfigs>,
     *    which specify table names, the number of primary keys, columns and rows:
     *
     *    CREATE TABLE <tableName> (P1 INT NOT NULL, ..., C1 DOUBLE, C2 DOUBLE, ...)
     *    [LOGICAL SPLITKEYS LOCATION '<fileName>']
     *
     *    Tables are automatically pre-split so that the size of each region does not
     *    exceed 2 GB but there is at least one region per node.
     *    Each table is populated with multiple concurrent connections doing INSERTs in batches
     *    which are randomly shuffled to ensure even load on all cluster nodes.
     */

    private static void createTables(int numHosts) throws Exception {
        try (Connection conn = makeConnection()) {
            try (Statement statement = conn.createStatement()) {
                for (TableConfig config : tableConfigs) {

                    LOG.info("Create table " + config.tableName);

                    Random rnd = ThreadLocalRandom.current();
                    String fileName = "/tmp/split_" + config.tableName;
                    StringBuilder sb = new StringBuilder();
                    char delimiter;

                    // Generate split keys
                    long rowBytes = 4 * config.numPK + 8 * config.numCols;
                    long rows2G   = (1L << 31) / rowBytes;
                    long chunks = Math.max(config.numRows / rows2G, numHosts);
                    boolean split = chunks > 1;
                    if (split) {
                        statement.execute("DROP TABLE IF EXISTS SPLITKEYS");
                        sb.setLength(0);
                        sb.append("CREATE TABLE SPLITKEYS ");
                        delimiter = '(';
                        for (int pk = 1; pk <= config.numPK; ++pk) {
                            sb.append(delimiter);
                            delimiter = ',';
                            sb.append("P").append(pk).append(" int");
                        }
                        sb.append(')');
                        statement.execute(sb.toString());

                        int[] PK = new int[config.numPK];
                        long splitRows = (config.numRows + chunks - 1) / chunks;
                        for (long row = splitRows; row < config.numRows; row += splitRows) {
                            sb.setLength(0);
                            sb.append("INSERT INTO SPLITKEYS VALUES ");
                            makePK(row, PK);
                            delimiter = '(';
                            for (int pk : PK) {
                                sb.append(delimiter);
                                delimiter = ',';
                                sb.append(pk);
                            }
                            sb.append(")");
                            statement.execute(sb.toString());
                        }
                        statement.execute(String.format("EXPORT('%s', false, null, null, null, null) SELECT * FROM SPLITKEYS", fileName));
                    }

                    // Create table
                    sb.setLength(0);
                    sb.append("CREATE TABLE ").append(config.tableName).append(" (");
                    for (int pk = 1; pk <= config.numPK; ++pk) {
                        sb.append("P").append(pk).append(" INT NOT NULL, ");
                    }
                    for (int c = 1; c <= config.numCols; ++c) {
                        sb.append("C").append(c).append(" DOUBLE, ");
                    }
                    sb.append(" PRIMARY KEY ");
                    delimiter = '(';
                    for (int pk = 1; pk <= config.numPK; ++pk) {
                        sb.append(delimiter);
                        delimiter = ',';
                        sb.append("P").append(pk);
                    }
                    sb.append("))");
                    if (split) {
                        sb.append(String.format("LOGICAL SPLITKEYS LOCATION '%s'", fileName));
                    }
                    statement.execute(sb.toString());

                    // Populate table
                    taskId.set(0);
                    batchId = new long[(int)((config.numRows + batchSize - 1) / batchSize)];
                    for (int i = 0; i < batchId.length; ++i) {
                        batchId[i] = i;
                    }
                    for (int i = batchId.length - 1; i > 0; --i) {
                        int j = rnd.nextInt(i + 1);
                        long tmp = batchId[i];
                        batchId[i] = batchId[j];
                        batchId[j] = tmp;
                    }
                    runBenchmark(connections, () -> populateTable(config));
                    if (abort) {
                        throw new RuntimeException("Execution aborted due to errors");
                    }
                }
                statement.execute("DROP TABLE IF EXISTS SPLITKEYS");
            }
        }
    }

    static final String STAT_ERROR = "ERROR";
    static final String STAT_CREATE = "CREATE";
    static final String STAT_QUERIES = "QUERIES";

    static final AtomicInteger taskId = new AtomicInteger(0);
    static final int batchSize = 1000;
    static long[] batchId;
    static boolean abort = false;

    private static void makePK(long id, int[] PK) {
        for (int i = 0; i < PK.length - 1; ++i) {
            PK[i] = (int)(id % 64);
            id /= 64;
        }
        PK[PK.length - 1] = (int)id;
    }

    private static void populateTable(TableConfig config) {
        try (Connection conn = makeConnection()) {
            StringBuilder sb = new StringBuilder();
            sb.append("INSERT INTO ").append(config.tableName).append(" VALUES (?");
            for (int i = 1; i < config.numPK + config.numCols; ++i) {
                sb.append(",?");
            }
            sb.append(")");
            PreparedStatement insert = conn.prepareStatement(sb.toString());
            Random rnd = ThreadLocalRandom.current();
            int[] PK = new int[config.numPK];

            for (;;) {
                int idx = taskId.getAndIncrement();
                if (idx >= batchId.length) break;
                long mini = batchId[idx] * batchSize;
                long maxi = Math.min(mini + batchSize, config.numRows);
                for (long i = mini; i < maxi; ++i) {
                    makePK(i, PK);
                    for (int pk = 0; pk < config.numPK; ++pk) {
                        insert.setInt(1 + pk, PK[pk]);
                    }

                    for (int col = 1; col <= config.numCols; ++col) {
                        insert.setDouble(config.numPK + col, rnd.nextDouble());
                    }
                    insert.addBatch();
                }
                long start = System.currentTimeMillis();
                int[] counts = insert.executeBatch();
                insert.clearBatch();
                long end = System.currentTimeMillis();
                int count = 0;
                for (int c : counts) count += c;
                if (count != maxi - mini) {
                    updateStats(STAT_ERROR);
                }
                if (count > 0) {
                    updateStats(STAT_CREATE, count, end - start);
                }
            }

            insert.close();
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
            abort = true;
        }
    }

    /*
     *    Benchmarks create a set of prebuilt queries that select from 1 to 9 of the feature set tables selecting 5 columns from each table.
     *    In a table with 10 columns, select columns c2, c4, c6, c8, c10, with 100 columns: c20, c40, c60, c80, c100 and with 1000: c200, c400, c600, c800, c1000.
     *
     *    SELECT <tableName>.<columnName>, ...
     *    FROM <tableName>, ...
     *    WHERE (<tableName>.<primaryKey>, ...) = (<random value>, ...)
     *
     *    Prepared queries are run from multiple concurrent connections for the specified number of operations per connection.
     *    Query results are read and hashed for possible validation (not implemented).
     *    Query execution times are collected to compute statistics provided by the general framework.
     */

    private static void runQueriesOrig(TableConfig[] configs) {
        try (Connection conn = makeConnection()) {
            Random rnd = ThreadLocalRandom.current();

            // Prepare the query
            int nColumns = 0;
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT");
            String delimiter = " ";
            for (TableConfig config : configs) {
                for (int col = 2; col <= 10; col += 2) {
                    sb.append(delimiter);
                    delimiter = ",";
                    sb.append(config.tableName).append('.').append("C").append(col * config.numCols / 10);
                    nColumns += 1;
                }
            }
            sb.append(" FROM");
            delimiter = " ";
            for (TableConfig config : configs) {
                sb.append(delimiter);
                delimiter = ",";
                sb.append(config.tableName);
            }
            sb.append(" WHERE");
            delimiter = " ";
            for (TableConfig config : configs) {
                sb.append(delimiter);
                delimiter = " AND ";

                char delimiter2 = '(';
                for (int pk = 1; pk <= config.numPK; ++pk) {
                    sb.append(delimiter2);
                    delimiter2 = ',';
                    sb.append(config.tableName).append(".P").append(pk);
                }
                sb.append(") = ");
                delimiter2 = '(';
                for (int pk = 0; pk < config.numPK; ++pk) {
                    sb.append(delimiter2);
                    delimiter2 = ',';
                    sb.append('?');
                }
                sb.append(')');
            }
            PreparedStatement select = conn.prepareStatement(sb.toString());

            for (int i = 0; i < operations; ++i) {
                long start = System.currentTimeMillis();
                try {
                    int idx = 1;
                    for (TableConfig config : configs) {
                        long id = Math.abs(rnd.nextLong()) % config.numRows;
                        int[] PK = new int[config.numPK];
                        makePK(id, PK);
                        for (int pk = 0; pk < config.numPK; ++pk) {
                            select.setInt(idx++, PK[pk]);
                        }
                    }

                    int rowCount = 0;
                    try (ResultSet rs = select.executeQuery()) {
                        while (rs.next()) {
                            ++rowCount;
                            long rowHash = 0;   //currently unused
                            for (int c = 1; c <= nColumns; ++c) {
                                Object obj = rs.getObject(c);
                                if (obj != null) {
                                    rowHash = (rowHash << 1) ^ (rowHash >>> 63) * 27L;
                                    rowHash ^= (obj.hashCode() & 0xffffffffL);
                                }
                            }
                        }
                    }
                    if (rowCount > 0) {
                        updateStats(STAT_QUERIES, rowCount, System.currentTimeMillis() - start);
                    }
                    else {
                        updateStats(STAT_ERROR);
                    }
                }
                catch (SQLException ex) {
                    LOG.error("ERROR", ex);
                    updateStats(STAT_ERROR);
                }
            }
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
            abort = true;
        }
    }

    /*
     *    Benchmarks create a set of prebuilt queries that select from 1 to 9 of the feature set tables selecting 5 columns from each table.
     *    In a table with 10 columns, select columns c2, c4, c6, c8, c10, with 100 columns: c20, c40, c60, c80, c100 and with 1000: c200, c400, c600, c800, c1000.
     *
     *    SELECT max(t1_c2), ... max(t2_c20), ... max(t3_c200), ... FROM (
     *      SELECT T1.c1 t1_c1, ... cast(NULL as int) t2_c20, ..., cast(NULL as int) t3_c200, ... FROM T1 WHERE (T1.<primaryKey>, ...) = (<random value>, ...)
     *      UNION ALL
     *      SELECT cast(NULL as int) t1_c1, ... T2.c20 t2_c20, ... cast(NULL as int) t3_c200, ... FROM T2 WHERE (T2.<primaryKey>, ...) = (<random value>, ...)
     *      ... )
     *
     *    Prepared queries are run from multiple concurrent connections for the specified number of operations per connection.
     *    Query results are read and hashed for possible validation (not implemented).
     *    Query execution times are collected to compute statistics provided by the general framework.
     */

    private static void runQueriesUnion(TableConfig[] configs) {
        try (Connection conn = makeConnection()) {
            Random rnd = ThreadLocalRandom.current();

            // Prepare the query
            int nColumns = 0;
            StringBuilder sb = new StringBuilder();
            sb.append("SELECT");
            String delimiter = " ";
            for (TableConfig config : configs) {
                for (int col = 2; col <= 10; col += 2) {
                    String colName = "C" + (col * config.numCols / 10);
                    sb.append(delimiter);
                    delimiter = ",";
                    sb.append("MAX(")
                      .append(config.tableName).append('_').append(colName)
                      .append(")");
                    nColumns += 1;
                }
            }
            delimiter= " FROM (";
            for (TableConfig config : configs) {
                sb.append(delimiter);
                delimiter = " UNION ALL ";

                String delimiter2 = "SELECT ";
                for (TableConfig config2 : configs) {
                    for (int col = 2; col <= 10; col += 2) {
                        String colName = "C" + (col * config2.numCols / 10);
                        sb.append(delimiter2);
                        delimiter2 = ",";
                        if (config2 == config) {
                            sb.append(config2.tableName).append(".").append(colName);
                        }
                        else {
                            sb.append(" CAST(NULL AS INT) ");
                        }
                        sb.append(" ").append(config2.tableName).append("_").append(colName);
                    }
                }
                sb.append(" FROM ").append(config.tableName);
                sb.append(" WHERE ");
                delimiter2 = "(";
                for (int pk = 1; pk <= config.numPK; ++pk) {
                    sb.append(delimiter2);
                    delimiter2 = ",";
                    sb.append(config.tableName).append(".P").append(pk);
                }
                sb.append(") = ");
                delimiter2 = "(";
                for (int pk = 0; pk < config.numPK; ++pk) {
                    sb.append(delimiter2);
                    delimiter2 = ",";
                    sb.append('?');
                }
                sb.append(')');
            }
            sb.append(')');
            PreparedStatement select = conn.prepareStatement(sb.toString());

            for (int i = 0; i < operations; ++i) {
                long start = System.currentTimeMillis();
                try {
                    int idx = 1;
                    for (TableConfig config : configs) {
                        long id = Math.abs(rnd.nextLong()) % config.numRows;
                        int[] PK = new int[config.numPK];
                        makePK(id, PK);
                        for (int pk = 0; pk < config.numPK; ++pk) {
                            select.setInt(idx++, PK[pk]);
                        }
                    }

                    int rowCount = 0;
                    try (ResultSet rs = select.executeQuery()) {
                        while (rs.next()) {
                            ++rowCount;
                            long rowHash = 0;   //currently unused
                            for (int c = 1; c <= nColumns; ++c) {
                                Object obj = rs.getObject(c);
                                if (obj != null) {
                                    rowHash = (rowHash << 1) ^ (rowHash >>> 63) * 27L;
                                    rowHash ^= (obj.hashCode() & 0xffffffffL);
                                }
                            }
                        }
                    }
                    if (rowCount > 0) {
                        updateStats(STAT_QUERIES, rowCount, System.currentTimeMillis() - start);
                    }
                    else {
                        updateStats(STAT_ERROR);
                    }
                }
                catch (SQLException ex) {
                    LOG.error("ERROR", ex);
                    updateStats(STAT_ERROR);
                }
            }
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
            abort = true;
        }
    }

    @Parameterized.Parameters
    public static Collection testParams() {
        return Arrays.asList(new Object[][] {{true}, {false}});
    }

    private boolean runOrig;

    public FeatureStoreBenchmark(boolean orig) {
        runOrig = orig;
    }

    @Before
    public void reset() {
        abort = false;
    }

    private String testTitle(TableConfig[] configs) {
        StringBuilder sb = new StringBuilder("Query");
        for (TableConfig config : configs) {
            sb.append(' ').append(config.tableName);
        }
        sb.append(runOrig ? " (orig)" : " (union)");
        return sb.toString();
    }

    private void runQueries(TableConfig[] tables) {
        LOG.info(testTitle(tables));

        long start = System.currentTimeMillis();
        runBenchmark(connections, () -> {
            if (runOrig) runQueriesOrig(tables);
            else runQueriesUnion(tables);
        });
        long end = System.currentTimeMillis();
        Assert.assertFalse(abort);

        long throughput = getCountStats(STAT_QUERIES) * 1000L / (end - start);
        LOG.info(STAT_QUERIES + " throughput: " + throughput + " ops/sec");
    }

    @Test
    public void queryT1() {
        runQueries(new TableConfig[] {T1});
    }

    @Test
    public void queryT2() {
        runQueries(new TableConfig[] {T2});
    }

    @Test
    public void queryT3() {
        runQueries(new TableConfig[] {T3});
    }

    @Test
    public void queryT1T2T3() {
        runQueries(new TableConfig[] {T1, T2, T3});
    }

    @Test
    public void queryT4T5T6() {
        runQueries(new TableConfig[] {T4, T5, T6});
    }

    @Test
    public void queryT7T8T9() {
        runQueries(new TableConfig[] {T7, T8, T9});
    }

    @Test
    public void queryT1T4T7() {
        runQueries(new TableConfig[] {T1, T4, T7});
    }

    @Test
    public void queryT2T5T8() {
        runQueries(new TableConfig[] {T2, T5, T8});
    }

    @Test
    public void queryT3T6T9() {
        runQueries(new TableConfig[] {T3, T6, T9});
    }

    @Test
    public void queryAllTables() {
        runQueries(new TableConfig[] {T1, T2, T3, T4, T5, T6, T7, T8, T9});
    }

}
