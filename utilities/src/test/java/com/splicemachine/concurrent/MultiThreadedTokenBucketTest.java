/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.concurrent;

import org.spark_project.guava.collect.Lists;
import com.splicemachine.concurrent.traffic.TrafficController;
import com.splicemachine.concurrent.traffic.TrafficShaping;
import com.splicemachine.metrics.ConcurrentEWMA;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 *         Date: 11/13/14
 */
public class MultiThreadedTokenBucketTest {

    int numThreads = 1;
    private ExecutorService executor;

    @Before
    public void setUp() throws Exception {
        executor = Executors.newFixedThreadPool(numThreads);
    }

    @After
    public void tearDown() throws Exception {
        executor.shutdownNow();
    }

    @Test(timeout=2000)
    public void testAllThreadsEventuallySucceed() throws Exception {
        final TrafficController tb = TrafficShaping.fixedRateTrafficShaper(10,1,TimeUnit.MILLISECONDS);

        List<Future<Void>> futures = Lists.newArrayList();
        final CountDownLatch startLatch = new CountDownLatch(1);
        for(int i=0;i<numThreads;i++){
            futures.add(executor.submit(new Callable<Void>() {
                @Override
                public Void call() throws Exception {
                    startLatch.await();
                    //now start
                    tb.acquire(50);
                    return null;
                }
            }));
        }

        //start them up
        startLatch.countDown();
        //wait for everyone to finid
        for(int i=0;i<futures.size();i++){
            futures.remove(0).get();
        }
    }

    @Test
    public void testKeepsOverallThroughputBelowBound() throws Exception {
        //if we have 1 token/millisecond, it should take a minimum of 1000 seconds to acquire 1000 permits,
        //even from multiple threads
        final TrafficController tb = TrafficShaping.fixedRateTrafficShaper(100000,100000,TimeUnit.SECONDS);

        List<Future<Integer>> futures = Lists.newArrayList();
        final CyclicBarrier startLatch = new CyclicBarrier(numThreads+1);
        final int iterCount = 10/numThreads;
        final ConcurrentEWMA throughput = new ConcurrentEWMA(60d,1,TimeUnit.SECONDS);
        for(int i=0;i<numThreads;i++){
            futures.add(executor.submit(new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                    startLatch.await();
                    //now start
                    int numPermits = 0;
                    int requestSize = 20000;
                    for(int i=0;i<iterCount;i++) {
                        tb.acquire(requestSize);
                        numPermits+=requestSize;
                        throughput.update(requestSize);
                        if(i%2==0)
                            System.out.printf("[%s] 1M rate: %f%n",Thread.currentThread().getName(),throughput.rate());
                    }
                    return numPermits;
                }
            }));
        }

        //start them up
        long time = System.currentTimeMillis();
        startLatch.await();
        //wait for everyone to finid
        int permits = 0;
        for(int i=0;i<futures.size();i++){
            permits+=futures.remove(0).get();
        }
        time = System.currentTimeMillis()-time;
        System.out.printf("permits: %d%n,time:%d%n,throughput:%f%n",permits,time,throughput.rate());
        Assert.assertTrue("Took too little time!time = " + time, time >=1000);
    }

}
