package com.splicemachine.stats.frequency;

import com.google.common.collect.Lists;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

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
        checkPowersOf8(po8Hitters);
        checkPowersOf4(byteCounter.heavyHitters(0.10f));
    }

    @Test
    public void testFrequentElementsCorrect() throws Exception {
        ByteFrequencyCounter byteCounter = new EnumeratingByteFrequencyCounter();
        fillPowersOf2(byteCounter,(byte)0xF8,(byte)0x08);

        /*
         * the two most frequent elements should be -8 and 0, which is the powersOf8
         */
        ByteFrequentElements mfe = byteCounter.frequentElements(2);
        checkPowersOf8(mfe);
        checkPowersOf4(byteCounter.frequentElements(4));
    }

    /***********************************************************************************************/
    /*private helper methods*/

    private void checkPowersOf4(ByteFrequentElements po4Hitters){
        //make sure -8 is returned
        Assert.assertEquals("Incorrect estimated count for -8!", 15, po4Hitters.countEqual((byte) -8).count());
        //make sure -4 is returned
        Assert.assertEquals("Incorrect estimated count for -4!", 7, po4Hitters.countEqual((byte) -4).count());
        //make sure 0 is returned
        Assert.assertEquals("Incorrect estimated count for 0!",15,po4Hitters.countEqual((byte)0).count());
        //make sure 4 is returned
        Assert.assertEquals("Incorrect estimated count for 4!", 7, po4Hitters.countEqual((byte) 4).count());

        //check -Infinity to -8
        assertEmptyBefore(po4Hitters, (byte) -8, false);
        List<TestFrequency> correct = Arrays.asList(new TestFrequency((byte)-8,15));
        checkRange("Incorrect frequencies for (-Inf,-8]", correct, po4Hitters.frequentBefore((byte) -8,true));

        //check the range from -8 to -4
        assertEmptyBetween(po4Hitters, (byte) -8, (byte) -4, false, false);
        correct = Lists.newArrayList( new TestFrequency((byte)-8,15), new TestFrequency((byte)-4,7));
        checkRange("Incorrect frequencies for [-8,-4]",correct,po4Hitters.frequentBetween((byte)-8,(byte)-4,true,true));
        correct = Arrays.asList(new TestFrequency((byte)-8,15));
        checkRange("Incorrect frequencies for [-8,-4)",correct,po4Hitters.frequentBetween((byte)-8,(byte)-4,true,false));

        //check from -4 to 0
        assertEmptyBetween(po4Hitters, (byte) -4, (byte) 0, false, false);
        correct = Arrays.asList(new TestFrequency((byte)-4,7),new TestFrequency((byte)0,15));
        checkRange("Incorrect frequencies for [-4,0]",correct,po4Hitters.frequentBetween((byte)-4,(byte)0,true,true));
        correct = Arrays.asList(new TestFrequency((byte)-4,7));
        checkRange("Incorrect frequencies for [-4,0)",correct,po4Hitters.frequentBetween((byte)-4,(byte)0,true,false));
        correct = Arrays.asList(new TestFrequency((byte)0,15));
        checkRange("Incorrect frequencies for (-4,0]",correct,po4Hitters.frequentBetween((byte)-4,(byte)0,false,true));

        //check from 0 to 4
        assertEmptyBetween(po4Hitters, (byte) 0, (byte) 4, false, false);
        correct = Arrays.asList(new TestFrequency((byte)4,7),new TestFrequency((byte)0,15));
        checkRange("Incorrect frequencies for [0,4]",correct,po4Hitters.frequentBetween((byte)0,(byte)4,true,true));
        correct = Arrays.asList(new TestFrequency((byte)0,15));
        checkRange("Incorrect frequencies for [0,4)",correct,po4Hitters.frequentBetween((byte)0,(byte)4,true,false));
        correct = Arrays.asList(new TestFrequency((byte)4,7));
        checkRange("Incorrect frequencies for (0,4]",correct,po4Hitters.frequentBetween((byte)0,(byte)4,false,true));

        correct = Arrays.asList(new TestFrequency((byte)4,7));
        checkRange("Incorrect frequencies for (0,4]",correct,po4Hitters.frequentAfter((byte) 4, true));
        //nothing after 4
        assertEmptyAbove(po4Hitters, (byte) 4, false);

        //check the range -8 to 0
        correct = Arrays.asList(new TestFrequency((byte)-8,15),new TestFrequency((byte)-4,7),new TestFrequency((byte)0,15));
        checkRange("Incorrect frequencies for [-8,0]",correct, po4Hitters.frequentBetween((byte)-8,(byte)0,true,true));
        correct = Arrays.asList(new TestFrequency((byte)-8,15),new TestFrequency((byte)-4,7));
        checkRange("Incorrect frequencies for [-8,0)",correct, po4Hitters.frequentBetween((byte)-8,(byte)0,true,false));
        correct = Arrays.asList(new TestFrequency((byte)-4,7),new TestFrequency((byte)0,15));
        checkRange("Incorrect frequencies for (-8,0]",correct, po4Hitters.frequentBetween((byte)-8,(byte)0,false,true));
        correct = Arrays.asList(new TestFrequency((byte)-4,7));
        checkRange("Incorrect frequencies for (-8,0)",correct, po4Hitters.frequentBetween((byte)-8,(byte)0,false,false));

        //check the range 0 to 8
        correct = Arrays.asList(new TestFrequency((byte)4,7),new TestFrequency((byte)0,15));
        checkRange("Incorrect frequencies for [0,8]",correct, po4Hitters.frequentBetween((byte)0,(byte)8,true,true));
        correct = Arrays.asList(new TestFrequency((byte)4,7),new TestFrequency((byte)0,15));
        checkRange("Incorrect frequencies for [0,8)",correct, po4Hitters.frequentBetween((byte)0,(byte)8,true,false));
        correct = Arrays.asList(new TestFrequency((byte)4,7));
        checkRange("Incorrect frequencies for (0,8]",correct, po4Hitters.frequentBetween((byte)0,(byte)8,false,true));
        correct = Arrays.asList(new TestFrequency((byte)4,7));
        checkRange("Incorrect frequencies for (0,8)",correct, po4Hitters.frequentBetween((byte)0,(byte)8,false,false));
    }

    private void checkRange(String message, List<TestFrequency> correct, Collection<ByteFrequencyEstimate> actual){
        Assert.assertEquals(message+":size does not match",correct.size(),actual.size());
        List<ByteFrequencyEstimate> actualEstimates = Lists.newArrayList(actual);
        Collections.sort(correct);
        Collections.sort(actualEstimates);
        for(int i=0;i<correct.size();i++){
            boolean equals = correct.get(i).equals(actualEstimates.get(i));
            Assert.assertTrue(message + ":element at pos " + i + " is not correct!Expected:<" + correct.get(i) + ">,Actual:<" + actualEstimates.get(i) + ">", equals);
        }
    }

    private void checkPowersOf8(ByteFrequentElements po8Hitters) {
        //make sure -8 is returned
        Assert.assertEquals("Incorrect estimated count for -8!", 15, po8Hitters.countEqual((byte) -8).count());
        //make sure 0 is returned
        Assert.assertEquals("Incorrect estimated count for 0!",15,po8Hitters.countEqual((byte)0).count());

        //make sure there are none below -8
        assertEmptyBefore(po8Hitters, (byte) -8, false);
        assertEmptyAbove(po8Hitters, (byte) 0, false);
        assertEmptyBetween(po8Hitters, (byte) -8, (byte) 0, false, false);

        //make sure we are correct for the range
        Set<ByteFrequencyEstimate> frequencyEstimates = po8Hitters.frequentBetween((byte)-8,(byte)0,true,true);
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

    private void assertEmptyBetween(ByteFrequentElements po8Hitters, byte start, byte stop,
                                    boolean includeStart, boolean includeStop) {
        Set<ByteFrequencyEstimate> frequencyEstimates;//make sure that there is nothing in (-8,0)
        frequencyEstimates = po8Hitters.frequentBetween(start,stop,includeStart,includeStop);
        Assert.assertEquals("Returned elements in (-8,0)!", 0, frequencyEstimates.size());
        Assert.assertFalse("Empty iterator hasNext() is true!", frequencyEstimates.iterator().hasNext());
    }

    private void assertEmptyAbove(ByteFrequentElements po8Hitters,byte start,
                                  boolean includeStart) {
        Set<ByteFrequencyEstimate> frequencyEstimates;//make sure that there is nothing in (-8,0)
        frequencyEstimates = po8Hitters.frequentAfter(start,includeStart);
        Assert.assertEquals("Returned elements after "+start+"!", 0, frequencyEstimates.size());
        Assert.assertFalse("Empty iterator hasNext() is true!", frequencyEstimates.iterator().hasNext());
    }

    private void assertEmptyBefore(ByteFrequentElements po8Hitters,byte stop,
                                  boolean includeStop) {
        Set<ByteFrequencyEstimate> frequencyEstimates;//make sure that there is nothing in (-8,0)
        frequencyEstimates = po8Hitters.frequentBefore(stop, includeStop);
        Assert.assertEquals("Returned elements before "+stop+"!", 0, frequencyEstimates.size());
        Assert.assertFalse("Empty iterator hasNext() is true!", frequencyEstimates.iterator().hasNext());
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

    private static class TestFrequency implements ByteFrequencyEstimate{
        final byte value;
        final long count;

        public TestFrequency(byte value, long count) {
            this.value = value;
            this.count = count;
        }

        @Override public byte value() { return value; }
        @Override public int compareTo(ByteFrequencyEstimate o) { return value-o.value(); }
        @Override public Byte getValue() { return value; }
        @Override public long count() { return count; }
        @Override public long error() { return 0; }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof ByteFrequencyEstimate)) return false;

            ByteFrequencyEstimate that = (ByteFrequencyEstimate) o;

            return value == that.value() && count == that.count();
        }

        @Override
        public int hashCode() {
            int result = (int) value;
            result = 31 * result + (int) (count ^ (count >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "Frequency("+value+","+count+")";
        }
    }
}
