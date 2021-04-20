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

package com.splicemachine.test;

import com.splicemachine.derby.test.framework.SpliceNetConnection;
import com.splicemachine.derby.test.framework.SpliceWatcher;
import com.splicemachine.derby.test.framework.TestConnection;
import org.apache.log4j.Logger;
import org.junit.Assert;

import java.sql.*;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import static org.junit.Assert.fail;

public class Benchmark {

    public static final Logger LOG = Logger.getLogger(Benchmark.class);

    /*
     *    Statistics
     */

    static final int MAXSTATS = 64;
    static final boolean cumulative = true; // false - reset stats on every report
    static AtomicLongArray statsCnt = new AtomicLongArray(MAXSTATS);
    static AtomicLongArray statsSum = new AtomicLongArray(MAXSTATS);
    static AtomicLong nextReport = new AtomicLong(0);
    static long deadLine = Long.MAX_VALUE;
    static ConcurrentHashMap<String, Integer> indexMap = new ConcurrentHashMap<>(MAXSTATS);
    static AtomicInteger lastIdx = new AtomicInteger(0);


    public static void updateStats(String stat) {
        updateStats(stat, 1, 0);
    }

    public static void updateStats(String stat, long delta) {
        updateStats(stat, 1, delta);
    }

    public static void updateStats(String stat, long increment, long delta) {
        int idx = indexMap.computeIfAbsent(stat, key -> lastIdx.getAndIncrement());
        if (idx >= MAXSTATS) {
            throw new RuntimeException("ERROR: Too many statistics: " + idx);
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

    public static void reportStats() {
        if (indexMap.isEmpty()) {
            return;
        }
        StringBuilder sb = new StringBuilder().append("Statistics:");
        for (Map.Entry<String, Integer> entry : indexMap.entrySet()) {
            int idx = entry.getValue();
            long count = cumulative ? statsCnt.get(idx) : statsCnt.getAndSet(idx, 0);
            long time  = cumulative ? statsSum.get(idx) : statsSum.getAndSet(idx, 0);
            if (count != 0) {
                long avg = (time + count / 2) / count;
                sb.append("\n\t").append(entry.getKey());
                sb.append("\tops: ").append(count);
                sb.append("\ttime: ").append(time / 1000).append(" s");
                sb.append("\tavg: ").append(avg).append(" ms");
            }
        }
        LOG.info(sb.toString());
    }

    public static void resetStats() {
        reportStats();
        nextReport.set(0);
        statsCnt = new AtomicLongArray(statsCnt.length());
        statsSum = new AtomicLongArray(statsSum.length());
    }

    /*
     *    Setup
     */

    public static void getInfo() throws Exception {
        try (Connection connection = SpliceNetConnection.getDefaultConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CALL SYSCS_UTIL.SYSCS_GET_VERSION_INFO()");
                try (ResultSet rs = statement.getResultSet()) {
                    while (rs.next()) {
                        String host = rs.getString(1);
                        String release = rs.getString(2);
                        LOG.info("HOST: " + host + "  SPLICE: " + release);
                    }
                }
            }
            finally {
                connection.rollback();
            }
        }
    }

    /*
     *    Parallel execution
     */

    public static void runBenchmark(int numThreads, Runnable runnable) {
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
            LOG.error("Interrupted while waiting for threads to finish", ie);
        }
        resetStats();
    }

    /***
     * Executes `query` using `conn` and verifies `matchLines` against the query's result set.
     *
     * @param matchLines a map of the expected String occurrence (s) in each
     * line of the result set, e.g. { {1,['abc','def']}, {10, 'ghi'}} expects line 1
     * contains 'abc' and 'def', and line 10 contains 'ghi'.
     */
    public static void executeQueryAndMatchLines(Connection conn,
                                                 String query,
                                                 Map<Integer,String[]> matchLines) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(query)) {
            try (ResultSet resultSet = ps.executeQuery()) {
                int i = 0;
                int k = 0;
                while (resultSet.next()) {
                    i++;
                    for (Map.Entry<Integer, String[]> entry : matchLines.entrySet()) {
                        int level = entry.getKey();
                        if (level == i) {
                            String resultString = resultSet.getString(1);
                            for (String phrase : entry.getValue()) {
                                Assert.assertTrue("failed query at level (" + level + "): \n" + query + "\nExpected: " + phrase + "\nWas: "
                                        + resultString, resultString.contains(phrase));
                            }
                            k++;
                        }
                    }
                }
                if (k < matchLines.size())
                    fail("fail to match the given strings");
            }
        }
    }

    public static double extractCostFromPlanRow(String planRow) throws NumberFormatException {
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
}
