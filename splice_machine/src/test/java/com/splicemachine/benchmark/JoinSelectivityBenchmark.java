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
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.junit.Assert.assertTrue;

@Category(Benchmark.class)
@RunWith(Parameterized.class)
public class JoinSelectivityBenchmark extends Benchmark {

    abstract static class FrequencyGenerator {
        abstract List<Integer> replicate(int value);

        protected List<Integer> replicateInternal(int value, int frequency) {
            List<Integer> result = new ArrayList<>(frequency);
            for(int i = 0; i < frequency; i++) {
                result.add(value);
            }
            return result;
        }

        abstract long totalCount(int valueCount);

        abstract int frequencyOf(int value);
    }

    static class FixedFrequencyGenerator extends FrequencyGenerator {
        private final int frequency;
        FixedFrequencyGenerator(int frequency) {
            this.frequency = frequency;
        }
        @Override
        List<Integer> replicate(int value) {
            return replicateInternal(value, frequency);
        }

        @Override
        long totalCount(int valueCount) {
            return (long) valueCount * frequency;
        }

        @Override
        int frequencyOf(int value) {
            return frequency;
        }

        @Override
        public String toString() {
            return Integer.toString(frequency);
        }
    }

    static class ZipfFrequencyGenerator extends FrequencyGenerator {

        private final int s;
        private final int c;

        public ZipfFrequencyGenerator(int c, int s) {
            this.c = c;
            this.s = s;
        }

        @Override
        List<Integer> replicate(int value) {
            return replicateInternal(value, frequencyOf(value));
        }

        @Override
        long totalCount(int valueCount) {
            long result = 0;
            for(int i = 0; i < valueCount; i++) {
                result += frequencyOf(i);
            }
            return result;
        }

        @Override
        int frequencyOf(int value) {
            return (int)((double)c / Math.pow(value, s));
        }

        @Override
        public String toString() {
            return String.format("Zipf(%d, %d)", s, c);
        }
    }

    private static final Logger LOG = Logger.getLogger(JoinSelectivityBenchmark.class);

    private static final String SCHEMA = JoinSelectivityBenchmark.class.getSimpleName();
    private static final String OUTER_TABLE = "OUTER_TABLE";
    private static final String LEFT_TABLE = "LEFT_TABLE";
    private static final String RIGHT_TABLE = "RIGHT_TABLE";

    private static final int NUM_CONNECTIONS = 10;
    private static final int NUM_EXECS = 20;
    private static final int NUM_WARMUP_RUNS = 5;

    private static final String NESTED_LOOP = "NestedLoop";

    private final int outerSize;
    private final int leftSize;
    private final int rightSize;
    private final boolean onOlap;
    private final int batchSize;
    private final FrequencyGenerator leftFrequencyGenerator;
    private final FrequencyGenerator rightFrequencyGenerator;

    public JoinSelectivityBenchmark(int leftSize, int rightSize, boolean onOlap,
                                    FrequencyGenerator leftFrequencyGenerator,
                                    FrequencyGenerator rightFrequencyGenerator) {
        this.outerSize = 10;
        this.onOlap = onOlap;
        this.batchSize = Math.min(Math.min(leftSize, rightSize), 1000);
        this.leftFrequencyGenerator = leftFrequencyGenerator;
        this.rightFrequencyGenerator = rightFrequencyGenerator;
        this.leftSize = leftSize;
        this.rightSize = rightSize;
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
        testStatement.execute("CREATE TABLE " + LEFT_TABLE + " (col1 INTEGER, col2 INTEGER)");
        testStatement.execute("CREATE TABLE " + RIGHT_TABLE + " (col1 INTEGER, col2 INTEGER)");
        testStatement.execute("CREATE TABLE " + OUTER_TABLE + " (col1 INTEGER, col2 INTEGER)");

        curSize.set(0);
        runBenchmark(NUM_CONNECTIONS, () -> populateTable(OUTER_TABLE, outerSize, new FixedFrequencyGenerator(1)));

        curSize.set(0);
        runBenchmark(NUM_CONNECTIONS, () -> populateTable(LEFT_TABLE, leftSize, leftFrequencyGenerator));
        verifyRpv(LEFT_TABLE, leftFrequencyGenerator);

        curSize.set(0);
        runBenchmark(NUM_CONNECTIONS, () -> populateTable(RIGHT_TABLE, rightSize, rightFrequencyGenerator));
        verifyRpv(RIGHT_TABLE, rightFrequencyGenerator);

        testStatement.execute(String.format("call syscs_util.syscs_flush_table('%s', '%s')", SCHEMA, OUTER_TABLE));
        testStatement.execute(String.format("call syscs_util.syscs_flush_table('%s', '%s')", SCHEMA, LEFT_TABLE));
        testStatement.execute(String.format("call syscs_util.syscs_flush_table('%s', '%s')", SCHEMA, RIGHT_TABLE));

        LOG.info("Collect statistics");
        try (ResultSet rs = testStatement.executeQuery("ANALYZE SCHEMA " + SCHEMA)) {
            assertTrue(rs.next());
        }
    }

    @After
    public void tearDown() throws Exception {
        testStatement.execute("DROP TABLE " + OUTER_TABLE);
        testStatement.execute("DROP TABLE " + LEFT_TABLE);
        testStatement.execute("DROP TABLE " + RIGHT_TABLE);
        testStatement.close();
        testConnection.close();
    }

    static final String STAT_ERROR = "ERROR";
    static final String STAT_PREP = "PREPARE ";
    static final String KEY_JOIN = " JOIN ON KEY";
    static AtomicInteger curSize = new AtomicInteger(0);

    private void populateTable(String tableName, int distinctRowCount, FrequencyGenerator frequencyGenerator) {
        try (Connection conn = makeConnection()) {
            try (PreparedStatement insert = conn.prepareStatement("INSERT INTO " + tableName + " VALUES (?,?)")) {
                for (; ; ) {
                    int newSize = curSize.getAndAdd(batchSize);
                    if (newSize >= distinctRowCount) break;
                    int freqCounter = 0;
                    for (int i = newSize; i < newSize + batchSize; ++i) {
                        List<Integer> rpv = frequencyGenerator.replicate(i);
                        for(int v : rpv) {
                            insert.setInt(1, v);
                            insert.setInt(2, v);
                            insert.addBatch();
                            if(freqCounter++ > batchSize) {
                                long start = System.currentTimeMillis();
                                int[] counts = insert.executeBatch();
                                long end = System.currentTimeMillis();
                                int count = 0;
                                for (int c : counts) count += c;
                                if (count > 0) {
                                    updateStats(STAT_PREP + tableName + " frequency-batch", count, end - start);
                                }
                                freqCounter = 0;
                            }
                            if(freqCounter > batchSize) {
                                long start = System.currentTimeMillis();
                                int[] counts = insert.executeBatch();
                                long end = System.currentTimeMillis();
                                int count = 0;
                                for (int c : counts) count += c;
                                if (count > 0) {
                                    updateStats(STAT_PREP + tableName + " frequency-batch", count, end - start);
                                }
                                freqCounter = 0;
                            }
                        }
                    }
                    long start = System.currentTimeMillis();
                    int[] counts = insert.executeBatch();
                    long end = System.currentTimeMillis();
                    int count = 0;
                    for (int c : counts) count += c;
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

    private void verifyRpv(String tableName, FrequencyGenerator frequencyGenerator) {
        try (Connection conn = makeConnection()) {
            try (PreparedStatement statement = conn.prepareStatement(String.format("SELECT col1, COUNT(col1) FROM %s GROUP BY col1 ORDER BY col1 ASC", tableName))) {
                try(ResultSet resultSet = statement.executeQuery()) {
                    while(resultSet.next()) {
                        int i = resultSet.getInt(1);
                        int actualRpv = resultSet.getInt(2);
                        int expectedRpv = frequencyGenerator.frequencyOf(i);
                        if(actualRpv != expectedRpv) {
                            LOG.error(String.format("Incorrect RPV in table %s for row %d having actual RPV of %d while expected RPC is %d", tableName, i, actualRpv, expectedRpv));
                        }
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


    private String[] parseCostLine(String costLine) {
        Pattern p = Pattern.compile("((\\d+.\\d+)|(\\d+))");
        Matcher m = p.matcher(costLine);
        String[] result = new String[2];
        int counter = 0, costIdx = 1, rowCountIdx = 2;
        double cost = 0.0d;
        int rowCount = 0;
        while(m.find()) {
            if(counter == costIdx) {
                result[0] = m.group(1);
            } else if(counter == rowCountIdx) {
                result[1] = m.group(1);
            }
            counter++;
        }
        return result;
    }

    private String[] getEstimatedCostUsing(Connection conn, String query, int lineNum, String costModel) {
        try (Statement statement = conn.createStatement()) {
            statement.execute(String.format("set session_property costModel='%s'", costModel));
        } catch (Throwable t) {
            LOG.error("Could not set session property!", t);
            return new String[2];
        }
        try (PreparedStatement ps = conn.prepareStatement(query)) {
            try (ResultSet resultSet = ps.executeQuery()) {
                int i = 0;
                while (resultSet.next()) {
                    if(i == lineNum) {
                        if(!resultSet.getString(1).contains("Broadcast")) {
                            LOG.error(String.format("Could not find join cost estimation for statement %s at line %d with costModel %s",
                                                    query, lineNum, costModel));
                            return new String[2];
                        }
                        return parseCostLine(resultSet.getString(1));
                    }
                    i++;
                }
                LOG.error(String.format("Could not find join cost estimation for statement %s at line %d with costModel %s",
                                        query, lineNum, costModel));
            }
            return new String[2];
        }
        catch (Throwable t) {
            LOG.error("Connection broken", t);
            return new String[2];
        }
    }

    private void benchmark(String joinStrategy, boolean isKeyJoin, int numExec, boolean warmUp, boolean onOlap) {
        String sqlText = String.format("select count(*) from %s A, (select %s.col1 from --splice-properties joinOrder=fixed\n" +
                              "%s, %s--splice-properties joinStrategy=broadcast, useOLAP=%b\n" +
                              "where %s.col1 = %s.col1) B --splice-properties joinStrategy=broadcast\n" +
                              "WHERE A.col1 = B.col1", OUTER_TABLE, LEFT_TABLE, LEFT_TABLE, RIGHT_TABLE,
                                       onOlap, LEFT_TABLE, RIGHT_TABLE);
        String dataLabel;
        dataLabel = String.format("on OLAP = %b, Left Count = %d, Left RPV = %s, Right Count = %d, Right RPV = %s", onOlap,
                                  leftSize, leftFrequencyGenerator.toString(), rightSize, rightFrequencyGenerator.toString());
        try (Connection conn = makeConnection()) {
            try (PreparedStatement query = conn.prepareStatement(sqlText)) {
                // validate plan
                Map<Integer, String[]> matchLines = new HashMap<>(1);
                matchLines.put(8, new String[]{"Broadcast"});
                executeQueryAndMatchLines(conn, "explain " + sqlText, matchLines);

                // report cost using v1 and v2 cost models.
                String[] costRowCountV1 = getEstimatedCostUsing(conn, "explain " + sqlText, 5, "v1");
                dataLabel += String.format(" estimated cost v1 = %s, estimated row count v1 = %s", costRowCountV1[0], costRowCountV1[1]);
                String[] costRowCountV2 = getEstimatedCostUsing(conn, "explain " + sqlText, 5, "v2");
                dataLabel += String.format(" estimated cost v2 = %s, estimated row count v2 = %s", costRowCountV2[0], costRowCountV2[1]);

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
                        if (getRowCount(rs) < outerSize) {
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
                { 10, 10, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100, 10, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000, 10, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10000, 10, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100000, 10, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000000, 10, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10, 100, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100, 100, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000, 100, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10000, 100, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100000, 100, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000000, 100, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10, 1000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100, 1000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000, 1000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10000, 1000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100000, 1000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000000, 1000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10, 10000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100, 10000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000, 10000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10000, 10000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100000, 10000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000000, 10000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10, 100000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100, 100000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000, 100000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10000, 100000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10, 1000000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100, 1000000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000, 1000000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10000, 1000000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10, 10, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100, 10, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000, 10, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10000, 10, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100000, 10, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000000, 10, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10, 100, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100, 100, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000, 100, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10000, 100, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100000, 100, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000000, 100, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10, 1000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100, 1000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000, 1000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10000, 1000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100000, 1000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000000, 1000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10, 10000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100, 10000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000, 10000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10000, 10000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100000, 10000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000000, 10000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10, 100000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100, 100000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000, 100000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10000, 100000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10, 1000000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 100, 1000000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 1000, 1000000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },
                { 10000, 1000000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(1) },

                { 10, 10, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100, 10, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000, 10, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10000, 10, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100000, 10, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000000, 10, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10, 100, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100, 100, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000, 100, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10000, 100, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100000, 100, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000000, 100, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10, 1000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100, 1000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000, 1000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10000, 1000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100000, 1000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000000, 1000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10, 10000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100, 10000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000, 10000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10000, 10000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100000, 10000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000000, 10000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10, 100000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100, 100000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000, 100000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10000, 100000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10, 1000000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100, 1000000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000, 1000000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10000, 1000000, false, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10, 10, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100, 10, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000, 10, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10000, 10, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100000, 10, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000000, 10, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10, 100, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100, 100, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000, 100, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10000, 100, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100000, 100, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000000, 100, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10, 1000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100, 1000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000, 1000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10000, 1000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100000, 1000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000000, 1000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10, 10000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100, 10000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000, 10000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10000, 10000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100000, 10000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000000, 10000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10, 100000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100, 100000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000, 100000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10000, 100000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10, 1000000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 100, 1000000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 1000, 1000000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },
                { 10000, 1000000, true, new FixedFrequencyGenerator(1), new FixedFrequencyGenerator(100) },


                { 10, 10, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100, 10, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000, 10, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10000, 10, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100000, 10, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000000, 10, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10, 100, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100, 100, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000, 100, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10000, 100, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100000, 100, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000000, 100, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10, 1000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100, 1000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000, 1000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10000, 1000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100000, 1000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000000, 1000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10, 10000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100, 10000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000, 10000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10000, 10000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100000, 10000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000000, 10000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10, 100000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100, 100000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000, 100000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10000, 100000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10, 1000000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100, 1000000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000, 1000000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10000, 1000000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10, 10, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100, 10, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000, 10, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10000, 10, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100000, 10, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000000, 10, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10, 100, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100, 100, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000, 100, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10000, 100, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100000, 100, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000000, 100, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10, 1000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100, 1000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000, 1000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10000, 1000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100000, 1000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000000, 1000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10, 10000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100, 10000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000, 10000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10000, 10000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100000, 10000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000000, 10000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10, 100000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100, 100000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000, 100000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10000, 100000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10, 1000000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 100, 1000000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 1000, 1000000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },
                { 10000, 1000000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(1) },

                { 10, 10, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100, 10, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000, 10, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10000, 10, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100000, 10, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000000, 10, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10, 100, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100, 100, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000, 100, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10000, 100, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100000, 100, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000000, 100, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10, 1000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100, 1000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000, 1000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10000, 1000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100000, 1000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000000, 1000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10, 10000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100, 10000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000, 10000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10000, 10000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100000, 10000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000000, 10000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10, 100000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100, 100000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000, 100000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10000, 100000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10, 1000000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100, 1000000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000, 1000000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10000, 1000000, false, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10, 10, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100, 10, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000, 10, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10000, 10, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100000, 10, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000000, 10, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10, 100, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100, 100, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000, 100, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10000, 100, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100000, 100, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000000, 100, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10, 1000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100, 1000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000, 1000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10000, 1000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100000, 1000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000000, 1000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10, 10000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100, 10000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000, 10000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10000, 10000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100000, 10000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000000, 10000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10, 100000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100, 100000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000, 100000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10000, 100000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10, 1000000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 100, 1000000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 1000, 1000000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
                { 10000, 1000000, true, new FixedFrequencyGenerator(100), new FixedFrequencyGenerator(100) },
        });
    }

    /* Warming up would bring data into hbase block cache. To run cold only benchmarks,
     * hbase block cache must be disabled first:
     * hfile.block.cache.size = 0
     */

    @Test
    public void JoinSelectivityTest() throws Exception {
        runBenchmark(1, () -> benchmark(NESTED_LOOP, true, NUM_EXECS, true, onOlap));
    }
}
