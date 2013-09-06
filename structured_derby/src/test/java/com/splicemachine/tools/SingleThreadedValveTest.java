package com.splicemachine.tools;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests basic functionality on a single thread for a Valve
 *
 * @author Scott Fines
 * Created on: 9/6/13
 */
public class SingleThreadedValveTest {
    @Test
    public void testTryAllowAllowsOneThrough() throws Exception {
        Valve valve = new Valve(new Valve.FixedMaxOpeningPolicy(1));
        Assert.assertTrue("valve does not allow through entries!",valve.tryAllow()>=0);
    }

    @Test
    public void testReleaseAllowsSubsequentAccepts() throws Exception {
        Valve valve = new Valve(new Valve.FixedMaxOpeningPolicy(1));
        Assert.assertTrue("valve does not allow through entries!", valve.tryAllow() >= 0);
        //make sure we can't get another
        Assert.assertFalse("valve allows through too many entries!",valve.tryAllow()>=0);

        //release an entry
        valve.release();

        //make sure we can now acquire
        Assert.assertTrue("valve does not allow through entries after release!",valve.tryAllow()>0);
    }

    @Test
    public void testAdjustUpwardsWorks() throws Exception {

        TestOpeningPolicy openingPolicy = new TestOpeningPolicy(1);
        Valve valve = new Valve(openingPolicy);
        Assert.assertTrue("valve does not allow through entries!",valve.tryAllow()>=0);
        //make sure we can't get another
        Assert.assertFalse("valve allows through too many entries!",valve.tryAllow()>=0);

        //increase the valve size
        openingPolicy.initialSize = 2;
        valve.adjustUpwards(0, Valve.OpeningPolicy.SizeSuggestion.INCREMENT);

        Assert.assertTrue("valve does not allow through entries!",valve.tryAllow()>=0);

        //make sure we can't get another
        Assert.assertFalse("valve allows through too many entries!",valve.tryAllow()>=0);
    }

    @Test
    public void testReduceWorks() throws Exception {

        TestOpeningPolicy openingPolicy = new TestOpeningPolicy(2);
        Valve valve = new Valve(openingPolicy);
        Assert.assertTrue("valve does not allow through entries!",valve.tryAllow()>=0);
        //make sure we can get two
        Assert.assertTrue("valve does not allow through entries!", valve.tryAllow() >= 0);

        //decrease the valve size
        openingPolicy.initialSize = 1;
        valve.reduceValve(0, Valve.OpeningPolicy.SizeSuggestion.DECREMENT);

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

    private class TestOpeningPolicy implements Valve.OpeningPolicy{

        private int initialSize;

        public TestOpeningPolicy(int initialSize) {
            this.initialSize = initialSize;
        }


        @Override
        public int reduceSize(int currentSize, SizeSuggestion suggestion) {
            return initialSize;
        }

        @Override
        public int allowMore(int currentSize, SizeSuggestion suggestion) {
            return initialSize;
        }
    }
}
