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