package com.splicemachine.tools;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests basic functionality on a single thread for a Valve
 *
 * @author Scott Fines
 * Created on: 9/6/13
 */
public class SingleThreadedSemaphoreValveTest {
    @Test
    public void testTryAllowAllowsOneThrough() throws Exception {
        Valve valve = new SemaphoreValve(new SemaphoreValve.FixedMaxOpeningPolicy(1));
        Assert.assertTrue("valve does not allow through entries!",valve.tryAllow()>=0);
    }

    @Test
    public void testReleaseAllowsSubsequentAccepts() throws Exception {
        Valve valve = new SemaphoreValve(new SemaphoreValve.FixedMaxOpeningPolicy(1));
        Assert.assertTrue("valve does not allow through entries!", valve.tryAllow() >= 0);
        //make sure we can't get another
        Assert.assertFalse("valve allows through too many entries!",valve.tryAllow()>=0);

        //release an entry
        valve.release();

        //make sure we can now acquire
        Assert.assertTrue("valve does not allow through entries after release!",valve.tryAllow()>=0);
    }

    @Test
    public void testAdjustUpwardsWorks() throws Exception {

        TestOpeningPolicy openingPolicy = new TestOpeningPolicy(1);
        SemaphoreValve valve = new SemaphoreValve(openingPolicy);
        Assert.assertTrue("valve does not allow through entries!",valve.tryAllow()>=0);
        //make sure we can't get another
        Assert.assertFalse("valve allows through too many entries!",valve.tryAllow()>=0);

        //increase the valve size
        openingPolicy.initialSize = 2;
        valve.adjustUpwards(0, Valve.SizeSuggestion.INCREMENT);

        Assert.assertTrue("valve does not allow through entries!",valve.tryAllow()>=0);

        //make sure we can't get another
        Assert.assertFalse("valve allows through too many entries!",valve.tryAllow()>=0);
    }

    @Test
    public void testReduceWorks() throws Exception {

        TestOpeningPolicy openingPolicy = new TestOpeningPolicy(2);
        SemaphoreValve valve = new SemaphoreValve(openingPolicy);
        Assert.assertTrue("valve does not allow through entries!",valve.tryAllow()>=0);
        //make sure we can get two
        Assert.assertTrue("valve does not allow through entries!", valve.tryAllow() >= 0);

        //decrease the valve size
        openingPolicy.initialSize = 1;
        valve.reduceValve(0, Valve.SizeSuggestion.DECREMENT);

        //make sure that we can't get another
        Assert.assertFalse("valve does not allow through entries!",valve.tryAllow()>=0);

        //release an entry
        valve.release();

        //make sure we still can't get another
        Assert.assertFalse("valve does not allow through entries!",valve.tryAllow()>=0);

        //release again
        valve.release();

        //we should now be able to acquire
        Assert.assertTrue("valve does not allow through entries!", valve.tryAllow() >= 0);
    }

    private class TestOpeningPolicy implements SemaphoreValve.OpeningPolicy{

        private int initialSize;

        public TestOpeningPolicy(int initialSize) {
            this.initialSize = initialSize;
        }


        @Override
        public int reduceSize(int currentSize, Valve.SizeSuggestion suggestion) {
            return initialSize;
        }

        @Override
        public int allowMore(int currentSize, Valve.SizeSuggestion suggestion) {
            return initialSize;
        }
    }
}
