/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.pipeline.traffic;

import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.ThreadLocalRandom;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Category(ArchitectureIndependent.class)
public class SpliceWriteControlTest {

    @Test
    public void performIndependentWrite() {
        SpliceWriteControl writeControl = new AtomicSpliceWriteControl(3, 3, 200, 200);

        writeControl.performIndependentWrite(25);
        assertEquals("{ dependentWriteThreads=0, independentWriteThreads=1, dependentWriteCount=0, independentWriteCount=25 }", writeControl.getWriteStatus().toString());

        writeControl.performIndependentWrite(25);
        assertEquals("{ dependentWriteThreads=0, independentWriteThreads=2, dependentWriteCount=0, independentWriteCount=50 }", writeControl.getWriteStatus().toString());

        writeControl.finishIndependentWrite(25);
        assertEquals("{ dependentWriteThreads=0, independentWriteThreads=1, dependentWriteCount=0, independentWriteCount=25 }", writeControl.getWriteStatus().toString());
    }

    private volatile boolean done;
    private final AtomicInteger curDependentThreads = new AtomicInteger();
    private final AtomicInteger curDependentCount = new AtomicInteger();
    private final AtomicInteger maxDependentThreads = new AtomicInteger();
    private final AtomicInteger maxDependentCount = new AtomicInteger();
    private final AtomicInteger curIndependentThreads = new AtomicInteger();
    private final AtomicInteger curIndependentCount = new AtomicInteger();
    private final AtomicInteger maxIndependentThreads = new AtomicInteger();
    private final AtomicInteger maxIndependentCount = new AtomicInteger();

    class BenchmarkThread extends Thread {

        int idx;
        SpliceWriteControl writeControl;
        int avgDependent;
        int avgIndependent;
        long numIter = 0;
        long rejected = 0;

        public BenchmarkThread(int idx, SpliceWriteControl writeControl) {
            this.idx = idx;
            this.writeControl = writeControl;
            avgDependent = writeControl.maxDependentWriteCount() / writeControl.maxDependendentWriteThreads();
            avgIndependent = writeControl.maxIndependentWriteCount() / writeControl.maxIndependentWriteThreads();
        }

        @Override
        public void run() {
            while (!done) {
                boolean dependent = ThreadLocalRandom.current().nextInt(3) == 0;
                int origin = dependent ? avgDependent/2 : avgIndependent/2;
                int numWrites = ThreadLocalRandom.current().nextInt(origin, 3 * origin);

                SpliceWriteControl.Status status = dependent ? writeControl.performDependentWrite(numWrites)
                        : writeControl.performIndependentWrite(numWrites);

                switch (status) {
                    case REJECTED:
                        ++rejected;
                        break;
                    case INDEPENDENT: {
                        int t = curDependentThreads.incrementAndGet();
                        int c = curDependentCount.addAndGet(numWrites);
                        maxDependentThreads.accumulateAndGet(t, (a, b) -> Math.max(a, b));
                        maxDependentCount.accumulateAndGet(c, (a, b) -> Math.max(a, b));
                        curDependentThreads.decrementAndGet();
                        curDependentCount.addAndGet(-numWrites);
                        writeControl.finishIndependentWrite(numWrites);
                        break;
                    }
                    case DEPENDENT: {
                        int t = curIndependentThreads.incrementAndGet();
                        int c = curIndependentCount.addAndGet(numWrites);
                        maxIndependentThreads.accumulateAndGet(t, (a, b) -> Math.max(a, b));
                        maxIndependentCount.accumulateAndGet(c, (a, b) -> Math.max(a, b));
                        curIndependentThreads.decrementAndGet();
                        curIndependentCount.addAndGet(-numWrites);
                        writeControl.finishDependentWrite(numWrites);
                        break;
                    }
                    default:
                        fail("Unknown status: " + status);
                        break;
                }

                ++numIter;
            }
        }
    }

//    @Test
    public void writeControlBenchmark() {
        int maxThreads = 160;
        int maxCount   = 32000;
        SpliceWriteControl writeControl = new AtomicSpliceWriteControl(maxThreads/2, maxThreads/2, maxCount/2, maxCount/2);

        BenchmarkThread[] threads = new BenchmarkThread[200];
        for (int i = 0; i < threads.length; ++i) {
            threads[i] = new BenchmarkThread(i, writeControl);
        }
        for (BenchmarkThread t : threads) {
            t.start();
        }

        try {
            Thread.sleep(30_000);
        }
        catch (InterruptedException ie) {
            System.out.println("ERROR: Benchmark has been interrupted");
        }

        done = true;
        for (BenchmarkThread t : threads) {
            try {
                t.join();
            }
            catch (InterruptedException ie) {}
        }

        WriteStatus status = writeControl.getWriteStatus();
        assertEquals(status.dependentWriteThreads, 0);
        assertEquals(status.dependentWriteCount, 0);
        assertEquals(status.independentWriteThreads, 0);
        assertEquals(status.independentWriteCount, 0);

        long totalSum = 0;
        long totalMin = Long.MAX_VALUE;
        long totalMax = 0;
        long successSum = 0;
        long successMin = Long.MAX_VALUE;
        long successMax = 0;

        for (BenchmarkThread t : threads) {
            totalSum += t.numIter;
            totalMin = Math.min(totalMin, t.numIter);
            totalMax = Math.max(totalMax, t.numIter);

            long success = t.numIter - t.rejected;
            successSum += success;
            successMin = Math.min(successMin, success);
            successMax = Math.max(successMax, success);
        }
        System.out.println("Dependent maxThreads:   " + writeControl.maxDependendentWriteThreads() + " measured: " + maxDependentThreads.get());
        System.out.println("Dependent maxCount:     " + writeControl.maxDependentWriteCount() + " measured: " + maxDependentCount.get());
        System.out.println("Independent maxThreads: " + writeControl.maxIndependentWriteThreads() + " measured: " + maxIndependentThreads.get());
        System.out.println("Independent maxCount:   " + writeControl.maxIndependentWriteCount() + " measured: " + maxIndependentCount.get());
        System.out.println("Total requests (min/avg/max):      " + totalMin + " / " + (totalSum + threads.length/2)/threads.length + " / " + totalMax);
        System.out.println("Successful requests (min/avg/max): " + successMin + " / " + (successSum + threads.length/2)/threads.length + " / " + successMax);
    }
}
