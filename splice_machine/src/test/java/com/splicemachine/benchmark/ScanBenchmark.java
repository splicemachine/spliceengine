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
public class ScanBenchmark extends Benchmark {

    private static final Logger LOG = Logger.getLogger(ScanBenchmark.class);

    private static final String SCHEMA = ScanBenchmark.class.getSimpleName();
    private static final String BASE_TABLE = "BASE_TABLE";
    private static final int NUM_CONNECTIONS = 10;
    private static final int NUM_EXECS = 20;
    private static final int NUM_WARMUP_RUNS = 5;

    private static final int TABLE_DEF = 0;
    private static final int INSERT_PARAM = 2;

    private final int numTableColumns;
    private final int splitCount;
    private final int tableSize;
    private final boolean onOlap;

    private final int batchSize;
    private final String tableDefStr;
    private final String insertParamStr;
    private final boolean costOnly;

    public ScanBenchmark(int numTableColumns, int splitCount, int tableSize, boolean onOlap) {
        this.numTableColumns = Math.max(numTableColumns, 1);
        this.tableSize = tableSize;
        this.onOlap = onOlap;
        this.splitCount = splitCount;

        this.batchSize = Math.min(tableSize, 1000);
        this.tableDefStr = getColumnDef(this.numTableColumns, TABLE_DEF);
        this.insertParamStr = getColumnDef(this.numTableColumns, INSERT_PARAM);
        this.costOnly = Boolean.parseBoolean(System.getProperty("costOnly"));
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
        testStatement.execute("CREATE TABLE " + BASE_TABLE + tableDefStr);

        splitTable(splitCount);

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
        testStatement.close();
        testConnection.close();
        reportStats();
    }

    static String toHBaseEscaped(String s) {
        if(s == null) {
            return null;
        }
        if(s.length() % 2 != 0) {
            throw new IllegalArgumentException("argument length must be an even number");
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < s.length(); i += 2) {
            sb.append("\\x").append(s, i, i+2);
        }
        return sb.toString();
    }

    private int getNumSplits(Connection c) throws SQLException {
        int cnt = 0;
        String getRegionsSQL = String.format("CALL SYSCS_UTIL.GET_REGIONS('%s', '%s', null,null, null, '|',null,null,null,null)",
                                             SCHEMA, BASE_TABLE);
        try (ResultSet rs = c.createStatement().executeQuery(getRegionsSQL)) {

            while (rs.next()) {
                LOG.info(String.format("Region %s, %s", rs.getString(2), rs.getString(3)));
                cnt++;
            }
        }
        return cnt;
    }

    private String rowIdOf(Connection conn, int pkColValue) {
        // remove once we figure out how to calculate rowid correctly.
        try(Statement s = conn.createStatement()) {
            s.execute(String.format("CREATE TABLE %s.TEMP_TABLE(col1 int primary key)", SCHEMA));
            s.execute(String.format("INSERT INTO %s.TEMP_TABLE VALUES %d", SCHEMA, pkColValue));
            try(ResultSet rs = s.executeQuery(String.format("SELECT ROWID FROM %s.TEMP_TABLE where col1 = %d", SCHEMA, pkColValue))) {
                rs.next();
                return rs.getString(1);
            }
        } catch(Throwable t) {
            LOG.error("Error running statement to get RowId", t);
            System.exit(-1);
            return "?";
        } finally {
            try {
                conn.createStatement().execute(String.format("DROP TABLE %s.TEMP_TABLE", SCHEMA));
            } catch (Exception e) {
                // ignore.
            }
        }
    }

    private void splitAt(Connection conn, String splits) {
        String splitAtSql = null;
        splitAtSql = String.format("call SYSCS_UTIL.syscs_split_table_or_index_at_points('%s', '%s', null,'%s')",
                                   SCHEMA, BASE_TABLE, splits);
        try (Statement statement = conn.createStatement()) {
            statement.execute(splitAtSql);
        } catch (Throwable t) {
            LOG.error("Error running statement to split", t);
            System.exit(-1);
        }
    }

    private void splitRegions(Connection conn, int numSplits) {
        assert numSplits != 0;
        int splitSize = tableSize / numSplits;
        String splits = "";
        for(int i = 1; i < numSplits; i++) {
            splits += toHBaseEscaped(rowIdOf(conn, splitSize * i)) + ",";
        }
        splitAt(conn, splits.substring(0, splits.length() - 1));
    }

    static final String STAT_ERROR = "ERROR";
    static final String STAT_CREATE = "CREATE";

    static AtomicInteger curSize = new AtomicInteger(0);

    private void populateTables(int size) {
        try (Connection conn = makeConnection()) {
            try (PreparedStatement insert1 = conn.prepareStatement("INSERT INTO " + BASE_TABLE + insertParamStr)) {
                for (; ; ) {
                    int newSize = curSize.getAndAdd(batchSize);
                    if (newSize >= size) break;
                    for (int i = newSize; i < newSize + batchSize; ++i) {
                        for (int j = 1; j <= numTableColumns; ++j) {
                            insert1.setInt(j, i);
                        }
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
            System.exit(-1);
        }
    }

    private static String getColumnDef(int numColumns, int defStrType) {
        StringBuilder sb = new StringBuilder();
        if (defStrType == INSERT_PARAM) {
            sb.append(" values ");
        }
        sb.append(" (");
        for (int i = 0; i < numColumns; i++) {
            if (i != 0) {
                sb.append(", ");
            }
            switch (defStrType) {
                case TABLE_DEF:
                    sb.append("col_");
                    sb.append(i);
                    sb.append(" int");
                    if(i == 0) {
                        sb.append(" primary key");
                    }
                    break;
                case INSERT_PARAM:
                    sb.append("?");
                    break;
                default:
                    assert false : "unknown definition type";
                    break;
            }
        }
        sb.append(")");
        return sb.toString();
    }

    private static int getRowCount(ResultSet rs) throws SQLException {
        assertTrue(rs.next());
        return rs.getInt(1);
    }

    private static double extractCostFromPlanRow(String planRow) throws NumberFormatException {
        double cost = 0.0;
        if (planRow != null) {
            String costField = planRow.split(",")[1];
            if (costField != null) {
                String costStr = costField.split("=")[1];
                if (costStr != null) {
                    cost = Double.parseDouble(costStr);
                }
            }
        }
        return cost;
    }

    private void splitTable(int numSplits) {
        if (numSplits > 0) {
            try (Connection conn = makeConnection()) {
                getNumSplits(conn);
                LOG.info(String.format("After splitting to %d region(s)", numSplits));
                splitRegions(conn, numSplits);
                int actualSplitCount = getNumSplits(conn);
                if (numSplits != actualSplitCount) {
                    LOG.error(String.format("number of actual splits %d is not equal to what was requested (%d)", actualSplitCount, numSplits));
                }
            } catch (Throwable t) {
                LOG.error("Connection broken", t);
            }
        }
    }

    private void benchmark(int numSplits, int numExec, boolean warmUp, boolean onOlap) {
        String sqlText;
        String dataLabel = "Base table scan with OLAP = " + onOlap;
        sqlText = String.format("select count(col_0) from %s --splice-properties useSpark=%b", BASE_TABLE, onOlap);
        dataLabel += ", SPLITS = " + numSplits;
        try (Connection conn = makeConnection()) {
            double scanCost = 0.0d;
            try (PreparedStatement ps = conn.prepareStatement("explain " + sqlText)) {
                try (ResultSet resultSet = ps.executeQuery()) {
                    int i = 0;
                    while (resultSet.next()) {
                        i++;
                        String resultString = resultSet.getString(1);
                        if (i == 6) {
                            scanCost = extractCostFromPlanRow(resultString);
                        }
                    }
                }
                LOG.info(String.format("scanCost=%.3f", scanCost));
            }
            if (costOnly) {
                return;
            }
            try (PreparedStatement query = conn.prepareStatement(sqlText)) {
                if (warmUp) {
                    for (int i = 0; i < NUM_WARMUP_RUNS; ++i) {
                        query.executeQuery();
                    }
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
                        LOG.error("ERROR execution " + i + " of scan benchmark on " + BASE_TABLE + ": " + ex.getMessage());
                        updateStats(STAT_ERROR);
                    }
                }
            }
        } catch (Throwable t) {
            LOG.error("Connection broken", t);
        }
    }

    @Parameterized.Parameters
    public static Collection testParams() {
        return Arrays.asList(new Object[][]{
                {10, -1, 1000,    true},
                {10, -1, 5000,    true},
                {10, -1, 10000,   true},
                {10, -1, 15000,   true},
                {10, -1, 20000,   true},
                {10, -1, 25000,   true},
                {10, -1, 30000,   true},
                {10, -1, 35000,   true},
                {10, -1, 40000,   true},
                {10, -1, 45000,   true},
                {10, -1, 50000,   true},
                {10, -1, 100000,  true},
                {10, -1, 125000,  true},
                {10, -1, 150000,  true},
                {10, -1, 175000,  true},
                {10, -1, 200000,  true},
                {10, -1, 250000,  true},
                {10, -1, 300000,  true},
                {10, -1, 350000,  true},
                {10, -1, 400000,  true},
                {10, -1, 450000,  true},
                {10, -1, 500000,  true},
                {10, -1, 550000,  true},
                {10, -1, 600000,  true},
                {10, -1, 650000,  true},
                {10, -1, 700000,  true},
                {10, -1, 750000,  true},
                {10, -1, 800000,  true},
                {10, -1, 850000,  true},
                {10, -1, 900000,  true},
                {10, -1, 950000,  true},
                {10, -1, 1000000, true},

                {10, 2, 1000,    true},
                {10, 2, 5000,    true},
                {10, 2, 10000,   true},
                {10, 2, 15000,   true},
                {10, 2, 20000,   true},
                {10, 2, 25000,   true},
                {10, 2, 30000,   true},
                {10, 2, 35000,   true},
                {10, 2, 40000,   true},
                {10, 2, 45000,   true},
                {10, 2, 50000,   true},
                {10, 2, 100000,  true},
                {10, 2, 125000,  true},
                {10, 2, 150000,  true},
                {10, 2, 175000,  true},
                {10, 2, 200000,  true},
                {10, 2, 250000,  true},
                {10, 2, 300000,  true},
                {10, 2, 350000,  true},
                {10, 2, 400000,  true},
                {10, 2, 450000,  true},
                {10, 2, 500000,  true},
                {10, 2, 550000,  true},
                {10, 2, 600000,  true},
                {10, 2, 650000,  true},
                {10, 2, 700000,  true},
                {10, 2, 750000,  true},
                {10, 2, 800000,  true},
                {10, 2, 850000,  true},
                {10, 2, 900000,  true},
                {10, 2, 950000,  true},
                {10, 2, 1000000, true},

                {10, 4, 1000,    true},
                {10, 4, 5000,    true},
                {10, 4, 10000,   true},
                {10, 4, 15000,   true},
                {10, 4, 20000,   true},
                {10, 4, 25000,   true},
                {10, 4, 30000,   true},
                {10, 4, 35000,   true},
                {10, 4, 40000,   true},
                {10, 4, 45000,   true},
                {10, 4, 50000,   true},
                {10, 4, 100000,  true},
                {10, 4, 125000,  true},
                {10, 4, 150000,  true},
                {10, 4, 175000,  true},
                {10, 4, 200000,  true},
                {10, 4, 250000,  true},
                {10, 4, 300000,  true},
                {10, 4, 350000,  true},
                {10, 4, 400000,  true},
                {10, 4, 450000,  true},
                {10, 4, 500000,  true},
                {10, 4, 550000,  true},
                {10, 4, 600000,  true},
                {10, 4, 650000,  true},
                {10, 4, 700000,  true},
                {10, 4, 750000,  true},
                {10, 4, 800000,  true},
                {10, 4, 850000,  true},
                {10, 4, 900000,  true},
                {10, 4, 950000,  true},
                {10, 4, 1000000, true},

                {10, 8, 1000,    true},
                {10, 8, 5000,    true},
                {10, 8, 10000,   true},
                {10, 8, 15000,   true},
                {10, 8, 20000,   true},
                {10, 8, 25000,   true},
                {10, 8, 30000,   true},
                {10, 8, 35000,   true},
                {10, 8, 40000,   true},
                {10, 8, 45000,   true},
                {10, 8, 50000,   true},
                {10, 8, 100000,  true},
                {10, 8, 125000,  true},
                {10, 8, 150000,  true},
                {10, 8, 175000,  true},
                {10, 8, 200000,  true},
                {10, 8, 250000,  true},
                {10, 8, 300000,  true},
                {10, 8, 350000,  true},
                {10, 8, 400000,  true},
                {10, 8, 450000,  true},
                {10, 8, 500000,  true},
                {10, 8, 550000,  true},
                {10, 8, 600000,  true},
                {10, 8, 650000,  true},
                {10, 8, 700000,  true},
                {10, 8, 750000,  true},
                {10, 8, 800000,  true},
                {10, 8, 850000,  true},
                {10, 8, 900000,  true},
                {10, 8, 950000,  true},
                {10, 8, 1000000, true},

                {10, 16, 1000,    true},
                {10, 16, 5000,    true},
                {10, 16, 10000,   true},
                {10, 16, 15000,   true},
                {10, 16, 20000,   true},
                {10, 16, 25000,   true},
                {10, 16, 30000,   true},
                {10, 16, 35000,   true},
                {10, 16, 40000,   true},
                {10, 16, 45000,   true},
                {10, 16, 50000,   true},
                {10, 16, 100000,  true},
                {10, 16, 125000,  true},
                {10, 16, 150000,  true},
                {10, 16, 175000,  true},
                {10, 16, 200000,  true},
                {10, 16, 250000,  true},
                {10, 16, 300000,  true},
                {10, 16, 350000,  true},
                {10, 16, 400000,  true},
                {10, 16, 450000,  true},
                {10, 16, 500000,  true},
                {10, 16, 550000,  true},
                {10, 16, 600000,  true},
                {10, 16, 650000,  true},
                {10, 16, 700000,  true},
                {10, 16, 750000,  true},
                {10, 16, 800000,  true},
                {10, 16, 850000,  true},
                {10, 16, 900000,  true},
                {10, 16, 950000,  true},
                {10, 16, 1000000, true},
        });
    }

    /* Warming up would bring data into hbase block cache. To run cold only benchmarks,
     * hbase block cache must be disabled first:
     * hfile.block.cache.size = 0
     */

    @Test
    public void baseTableScanWarmup() throws Exception {
        LOG.info("baseTableScanWarmup");

        runBenchmark(1, () -> benchmark(splitCount, NUM_EXECS, true, onOlap));
    }
}
