package com.splicemachine.stats.frequency;

import org.junit.Assert;
import org.junit.Test;

import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 12/7/14
 */
public class FixedByteFrequencyCounterTest {

    @Test
    public void testHeavyHittersCorrect() throws Exception {
        /*
         * We test that the heavy hitters works as follows:
         *
         * Generate 100 values, distributed over 10 values such
         * that
         * 5 have over 10 (50%)
         * 3 have 5 to 10 (30%)
         *
         * and the remainder have either 0 or 1. Then we get the
         * Heavy Hitters covering over 10%, and see if it matches
         */
        ByteFrequencyCounter byteCounter = new EnumeratingByteFrequencyCounter();
        fillPowersOf2(byteCounter,(byte)0xF8,(byte)0x08);

        //get the multiples of 8
        ByteFrequentElements po8Hitters = byteCounter.heavyHitters(0.20f);
        //make sure -8 is returned
        Assert.assertEquals("Incorrect estimated count for -8!",15,po8Hitters.countEqual((byte)-8).count());
        //make sure 0 is returned
        Assert.assertEquals("Incorrect estimated count for 0!",15,po8Hitters.countEqual((byte)0).count());

        //make sure there are none below -8
        Set<ByteFrequencyEstimate> frequencyEstimates = po8Hitters.frequentBefore((byte)-8, false);
        Assert.assertEquals("Returned elements below -8!",0,frequencyEstimates.size());
        Assert.assertFalse("Empty iterator hasNext() is true!",frequencyEstimates.iterator().hasNext());
        //make sure there isn't anything after 0
        frequencyEstimates = po8Hitters.frequentAfter((byte) 0, false);
        Assert.assertEquals("Returned elements below -8!",0,frequencyEstimates.size());
        Assert.assertFalse("Empty iterator hasNext() is true!", frequencyEstimates.iterator().hasNext());
        //make sure that there is nothing in (-8,0)
        frequencyEstimates = po8Hitters.frequentBetween((byte)-8,(byte)0,false,false);
        Assert.assertEquals("Returned elements in (-8,0)!",0,frequencyEstimates.size());
        Assert.assertFalse("Empty iterator hasNext() is true!", frequencyEstimates.iterator().hasNext());
        //make sure we are correct for the range
        frequencyEstimates = po8Hitters.frequentBetween((byte)-8,(byte)0,true,true);
        Assert.assertEquals("Incorrect elements for [-8,0] -8!", 2, frequencyEstimates.size());
        boolean found8 = false;
        boolean found0 = false;
        for(ByteFrequencyEstimate estimate:frequencyEstimates){
            if(estimate.value()==(byte)-8) found8=true;
            else if(estimate.value()==(byte)0)found0 = true;
            else
                Assert.fail("Found unexpected estimate!"+estimate);
        }
        Assert.assertTrue("did not find -8 when expected!",found8);
        Assert.assertTrue("did not find 0 when expected!",found0);

        frequencyEstimates = po8Hitters.frequentBetween((byte)-8,(byte)0,true,false);
        Assert.assertEquals("Incorrect elements for [-8,0)!",1,frequencyEstimates.size());
        found8 = false;
        for(ByteFrequencyEstimate estimate:frequencyEstimates){
            if(estimate.value()==(byte)-8) found8=true;
            else if(estimate.value()==(byte)0)Assert.fail("Unexpectedly found 0!");
            else
                Assert.fail("Found unexpected estimate!"+estimate);
        }
        Assert.assertTrue("did not find -8 when expected!", found8);

        frequencyEstimates = po8Hitters.frequentBetween((byte)-8,(byte)0,false,true);
        Assert.assertEquals("Incorrect elements for (-8,0]!",1,frequencyEstimates.size());
        found0 = false;
        for(ByteFrequencyEstimate estimate:frequencyEstimates){
            if(estimate.value()==(byte)-8) Assert.fail("Unexpectedly found -8!");
            else if(estimate.value()==(byte)0)found0=true;
            else
                Assert.fail("Found unexpected estimate!"+estimate);
        }
        Assert.assertTrue("did not find 0 when expected!", found0);
    }

    private void fillPowersOf2(ByteFrequencyCounter byteCounter,byte start, byte stop) {
         /*
          * Load up 64 elements, with distribution as follows:
          *
          * -8: (1+2+4+8) = 15 (23%)
          * -7: 1
          * -6: (1+2) = 3 (4%)
          * -5: 1
          * -4: (1+2+4) = 7 (11%)
          * -3: 1
          * -2: (1+2) = 3 (4%)
          * -1: 1
          *  0: (1+2+4+8) = 15 (23%)
          *  1: 1
          *  2: (1+2) = 3 (4%)
          *  3: 1
          *  4: (1+2+4) = 7 (11%)
          *  5: 1
          *  6: (1+2) = 3 (4%
          *  7: 1
          *
          *  So the heavy hitters with support 20% are -8,0,while the
          *  heavy hitters with support 10% are -8,-4,0,4, and the HH with
          *  support 4% are -8,-6,-4,-2,0,2,4,6
          */
        //update every entry with 1
        for(byte b=start;b<stop;b++){
            byteCounter.update(b);
            if(b%2==0) byteCounter.update(b,2);
            if(b%4==0) byteCounter.update(b,4);
            if(b%8==0) byteCounter.update(b,8);
        }
    }
}
