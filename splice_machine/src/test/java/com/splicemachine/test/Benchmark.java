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
import com.splicemachine.homeless.TestUtils;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.LogManager;
import org.junit.Assert;

import java.sql.*;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import static org.junit.Assert.fail;

public class Benchmark {

    public static final Logger LOG = LogManager.getLogger(Benchmark.class);

    /*
     *    Statistics
     */

    static final int MAXSTATS = 1024;
    static final boolean cumulative = true; // false - reset stats on every report
    static AtomicLongArray statsCnt;
    static AtomicLongArray statsSum;
    static AtomicLongArray statsMin;
    static AtomicLongArray statsMax;
    static final AtomicLong nextReport = new AtomicLong(0);
    static long deadLine = Long.MAX_VALUE;
    static ConcurrentHashMap<String, Integer> indexMap = new ConcurrentHashMap<>(MAXSTATS);
    static final AtomicInteger lastIdx = new AtomicInteger(0);

    static {
        resetStats();
    }


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
        if (increment > 0) {
            statsMin.accumulateAndGet(idx, delta / increment, Math::min);
            statsMax.accumulateAndGet(idx, (delta + increment - 1) / increment, Math::max);
        }

        long curTime = System.currentTimeMillis();
        long t = nextReport.get();
        if (curTime > t && nextReport.compareAndSet(t, (t == 0 ? curTime : t) + 60000) && t > 0) {
            reportStats();
            if (curTime > deadLine) {
                throw new RuntimeException("Deadline has been reached");
            }
        }
    }

    public static long getCountStats(String stat) {
        int idx = indexMap.getOrDefault(stat, -1);
        return idx >= 0 ? statsCnt.get(idx) : 0;
    }

    public static long getSumStats(String stat) {
        int idx = indexMap.getOrDefault(stat, -1);
        return idx >= 0 ? statsSum.get(idx) : 0;
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
            long min   = cumulative ? statsMin.get(idx) : statsMin.getAndSet(idx, Long.MAX_VALUE);
            long max   = cumulative ? statsMax.get(idx) : statsMax.getAndSet(idx, 0);
            if (count != 0) {
                sb.append("\n\t").append(entry.getKey());
                sb.append("\tops: ").append(count);
                if (time != 0) {
                    long avg = (time + count / 2) / count;
                    sb.append("\ttime(s): ").append(time / 1000);
                    sb.append("\tmin/avg/max(ms): [").append(min).append(" .. ").append(avg).append(" .. ").append(max).append("]");
                }
            }
        }
        LOG.info(sb.toString());
    }

    public static void resetStats() {
        nextReport.set(0);
        statsCnt = new AtomicLongArray(MAXSTATS);
        statsSum = new AtomicLongArray(MAXSTATS);
        statsMin = new AtomicLongArray(MAXSTATS);
        statsMax = new AtomicLongArray(MAXSTATS);
        for (int i = 0; i < statsMin.length(); ++i) {
            statsMin.set(i, Long.MAX_VALUE);
        }
    }

    /*
     *    Setup
     */

    public static class RegionServerInfo {
        public String hostName;
        public String release;
        public String buildHash;

        RegionServerInfo(String hostName, String release, String buildHash) {
            this.hostName  = hostName;
            this.release   = release;
            this.buildHash = buildHash;
        }
    }

    public static RegionServerInfo[] getRegionServerInfo() throws Exception {
        try (Connection connection = SpliceNetConnection.getDefaultConnection()) {
            try (Statement statement = connection.createStatement()) {
                statement.execute("CALL SYSCS_UTIL.SYSCS_GET_VERSION_INFO()");
                try (ResultSet rs = statement.getResultSet()) {
                    ArrayList<RegionServerInfo> info = new ArrayList<>();
                    while (rs.next()) {
                        info.add(new RegionServerInfo(rs.getString(1), rs.getString(2), rs.getString(3)));
                    }
                    return info.toArray(new RegionServerInfo[info.size()]);
                }
            }
            finally {
                connection.rollback();
            }
        }
    }

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
        resetStats();

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

        // Report final statistics
        reportStats();
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
                if (k < matchLines.size()) {
                    Assert.fail("fail to match the given strings");
                }
            }
        }
    }

    /***
     * Executes `query` using `conn` and verify the output contain `containedString`.
     *
     * @param containedString The string to search for in the query result.
     */
    public static void testQueryContains(Connection conn,
                                         String query,
                                         String containedString) throws Exception {
        try (PreparedStatement ps = conn.prepareStatement(query)) {
            try (ResultSet resultSet = ps.executeQuery()) {
                String queryOutputString = TestUtils.FormattedResult.ResultFactory.toStringUnsorted(resultSet);
            if (!queryOutputString.contains(containedString))
                fail("ResultSet does not contain string: " + containedString + "\nResult set:\n"+queryOutputString);
            }
        }
    }
}
