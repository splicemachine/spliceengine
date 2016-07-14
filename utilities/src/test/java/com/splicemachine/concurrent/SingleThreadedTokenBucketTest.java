/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.concurrent;

import com.splicemachine.concurrent.traffic.TrafficController;
import com.splicemachine.concurrent.traffic.TrafficShaping;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

public class SingleThreadedTokenBucketTest {

    @Test
    public void testTryAcquireWhenBucketIsFull() throws Exception {
        TrafficController tb = TrafficShaping.fixedRateTrafficShaper(10,1,TimeUnit.MILLISECONDS);
        Assert.assertTrue(tb.tryAcquire(5));
    }

    @Test
    public void testCannotAcquireWhenBucketIsEmpty() throws Exception {
        TrafficController tb = TrafficShaping.fixedRateTrafficShaper(10,1,TimeUnit.MILLISECONDS);
        Assert.assertTrue(tb.tryAcquire(10));

        Assert.assertFalse("Could acquire even though empty!",tb.tryAcquire(1));
    }

    @Test
    public void testCannotAcquireWhenAskingForMoreThanTheBucket() throws Exception {
        TrafficController tb = TrafficShaping.fixedRateTrafficShaper(10,1,TimeUnit.MILLISECONDS);
        Assert.assertFalse(tb.tryAcquire(11));
    }

    @Test
    public void testCanAcquireAfterWaiting() throws Exception {
        IncrementingClock c = new IncrementingClock();
        TrafficController tb = TrafficShaping.fixedRateTrafficShaper(10,1,TimeUnit.MILLISECONDS,c);
        Assert.assertTrue(tb.tryAcquire(10));
        Assert.assertFalse("Could acquire when full!", tb.tryAcquire(1));

        c.increment(TimeUnit.MILLISECONDS.toNanos(1));
        Assert.assertTrue(tb.tryAcquire(1));
    }

    @Test
    public void testRepeatedCanAcquireAfterWaiting() throws Exception{
        for(int i=0;i<1000;i++){
            testCanAcquireAfterWaiting();
        }
    }

    @Test(timeout=100)
    public void testCanEventuallyAcquireAllTheNeededPermits() throws Exception {
        TrafficController tb = TrafficShaping.fixedRateTrafficShaper(10,1,TimeUnit.MILLISECONDS);
        Assert.assertFalse("Could acquire more than desired",tb.tryAcquire(20));
        long t = System.currentTimeMillis();
        tb.acquire(20); //now acquire
        long diff = System.currentTimeMillis()-t;
        //should have required at least 10 milliseconds
        Assert.assertTrue("Did not take enough time",diff>=10);
    }

    @Test(timeout=100)
    public void testCanEventuallyAcquireAllTheNeededPermitsNanoSecondClock() throws Exception {
        TrafficController tb = TrafficShaping.fixedRateTrafficShaper(10,1,TimeUnit.NANOSECONDS);
        Assert.assertFalse("Could acquire more than desired",tb.tryAcquire(20));
        long t = System.nanoTime();
        tb.acquire(20); //now acquire
        long diff = System.nanoTime()-t;
        Assert.assertTrue("Did not take enough time",diff>=10);
    }


    @Test(timeout=100)
    public void testTimeoutWaitingForAllNeededPermits() throws Exception {
        TrafficController tb = TrafficShaping.fixedRateTrafficShaper(10,1,TimeUnit.MILLISECONDS);
        Assert.assertFalse("Could acquire more than desired",tb.tryAcquire(20));
        long t = System.currentTimeMillis();
        Assert.assertFalse("Did not timeout!",tb.tryAcquire(200, 10, TimeUnit.MILLISECONDS)); //should timeout
        long diff = System.currentTimeMillis()-t;
        //should have required at least 10 milliseconds
        Assert.assertTrue("Did not take enough time",diff>=10);
    }

    @Test(timeout=2000)
    public void testKeepsRequestsToMaxThroughput() throws Exception {
        /*
         * If we allow 1 token/millisecond, then the maximum number of conforming
         * actions that occurs within, say, 1 second, should be 1000+initialSize. Make sure that this happens
         */
        TrafficController tb = TrafficShaping.fixedRateTrafficShaper(1,1,TimeUnit.MILLISECONDS);

        //run 1000 iterations, then make sure that it took longer than 1 second
        long start = System.currentTimeMillis();
        for(int i=0;i<1000;i++){
            tb.acquire(1); //acquire one permit, waiting as long as necessary
        }
        long timeTaken = System.currentTimeMillis()-start;
        System.out.printf("Took %d ms%n", timeTaken);
        Assert.assertTrue("Didn't take long enough! Took: "+timeTaken,timeTaken>1000);
    }
}