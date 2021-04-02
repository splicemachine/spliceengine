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
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public class Benchmark {

    public static final Logger LOG = Logger.getLogger(Benchmark.class);

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
        statsMin.accumulateAndGet(idx, delta, Math::min);
        statsMax.accumulateAndGet(idx, delta, Math::max);

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
            long min   = cumulative ? statsMin.get(idx) : statsMin.getAndSet(idx, Long.MAX_VALUE);
            long max   = cumulative ? statsMax.get(idx) : statsMax.getAndSet(idx, 0);
            if (count != 0) {
                long avg = (time + count / 2) / count;
                sb.append("\n\t").append(entry.getKey());
                sb.append("\tops: ").append(count);
                sb.append("\ttime(s): ").append(time / 1000);
                sb.append("\tmin/avg/max(ms): [").append(min).append(" .. ").append(avg).append(" .. ").append(max).append("]");
            }
        }
        LOG.info(sb.toString());
    }

    public static void resetStats() {
        reportStats();
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
}
