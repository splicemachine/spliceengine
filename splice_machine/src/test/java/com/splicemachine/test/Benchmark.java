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

import org.apache.log4j.Logger;

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

    static final int MAXSTATS = 64;
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
        StringBuilder sb = new StringBuilder().append("Statistics:");
        for (Map.Entry<String, Integer> entry : indexMap.entrySet()) {
            int idx = entry.getValue();
            long count = statsCnt.getAndSet(idx, 0);
            long time  = statsSum.getAndSet(idx, 0);
            if (count != 0) {
                long avg = (time + count / 2) / count;
                sb.append("\n\t").append(entry.getKey());
                sb.append("\tcalls: ").append(count);
                sb.append("\ttime: ").append(time / 1000).append(" s");
                sb.append("\tavg: ").append(avg).append(" ms");
            }
        }
        LOG.info(sb.toString());
    }

    public static void resetStats() {
        reportStats();
        nextReport.set(0);
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
