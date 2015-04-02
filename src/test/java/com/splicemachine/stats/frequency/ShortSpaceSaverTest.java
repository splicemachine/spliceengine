package com.splicemachine.stats.frequency;

import com.google.common.collect.Lists;
import com.google.common.primitives.Shorts;
import com.splicemachine.hash.HashFunctions;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * @author Scott Fines
 *         Date: 2/18/15
 */
public class ShortSpaceSaverTest {

    @Test
    public void testFrequentElementsCorrectWithNoEviction() throws Exception {
        ShortFrequencyCounter counter = new ShortSpaceSaver(HashFunctions.murmur3(0), 100);

        fillPowersOf2(counter);

        ShortFrequentElements fe = counter.frequentElements(2);
        checkMultiplesOf8(fe);
        checkMultiplesOf4(counter.frequentElements(4));
    }

    @Test
    public void testFrequentElementsCorrectWithEviction() throws Exception {
        ShortFrequencyCounter counter = new ShortSpaceSaver(HashFunctions.murmur3(0), 3);

        fillPowersOf2(counter);

        ShortFrequentElements fe = counter.frequentElements(3);
        checkMultiplesOf8(fe);
//        checkMultiplesOf4(counter.frequentElements(4));
    }

    private void checkMultiplesOf8(ShortFrequentElements fe) {
        Assert.assertEquals("Incorrect value for -8!", 15, fe.equal((short)-8).count());
        Assert.assertEquals("Incorrect value for 0!", 15, fe.equal((short)0).count()-fe.equal((short)0).error());

        //check before -8
        assertEmpty("Values <-8 found!", fe.frequentBefore((short)-8, false));
        //check includes -8
        List<TestFrequency> correct = Arrays.asList(new TestFrequency((short)-8, 15, 0));
        assertMatches("Values <-8 found!", correct, fe.frequentBefore((short)-8, true));

        //check from -8 to 0
        correct = Arrays.asList(new TestFrequency(-8, 15, 0), new TestFrequency(0, 15, 0));
        assertMatches("Incorrect values for range [-8,0]!", correct, fe.frequentBetween((short)-8, (short)0, true, true));
        correct = Arrays.asList(new TestFrequency(-8, 15, 0));
        assertMatches("Incorrect values for range [-8,0)!", correct, fe.frequentBetween((short)-8, (short)0, true, false));
        correct = Arrays.asList(new TestFrequency(0, 15, 0));
        assertMatches("Incorrect values for range (-8,0]!", correct, fe.frequentBetween((short)-8, (short)0, false, true));
        correct = Collections.emptyList();
        assertMatches("Incorrect values for range (-8,0]!", correct, fe.frequentBetween((short)-8, (short)0, false, false));

        //check from 0 to 8
//        assertMatches("Values >0 found!", Collections.<TestFrequency>emptyList(), fe.frequentAfter((short)0, false));
//        correct = Arrays.asList(new TestFrequency(0, 15, 0));
//        assertMatches("Incorrect values for range [0,8]!", correct, fe.frequentBetween((short)0, (short)8, true, true));
//        assertMatches("Incorrect values for range [0,8)!", correct, fe.frequentBetween((short)0, (short)8, true, false));
//        correct = Collections.emptyList();
//        assertMatches("Incorrect values for range (0,8]!", correct, fe.frequentBetween((short)0, (short)8, false, true));
//        assertMatches("Incorrect values for range (0,8)!", correct, fe.frequentBetween((short)0, (short)8, false, false));
    }

    private void checkMultiplesOf4(ShortFrequentElements fe) {
        Assert.assertEquals("Incorrect value for -8!", 15, fe.equal((short)-8).count());
        Assert.assertEquals("Incorrect value for -4!", 7, fe.equal((short)-4).count() - fe.equal((short)-4).error());
        Assert.assertEquals("Incorrect value for 0!", 15, fe.equal((short)0).count());
        Assert.assertEquals("Incorrect value for -4!", 7, fe.equal((short)4).count() - fe.equal((short)4).error());

        //check before -8
        assertEmpty("Values <-8 found!", fe.frequentBefore((short)-8, false));
        //check includes -8
        List<TestFrequency> correct = Arrays.asList(new TestFrequency(-8, 15, 0));
        assertMatches("Values <-8 found!", correct, fe.frequentBefore((short)-8, true));

        //check from -8 to -4
        correct = Arrays.asList(new TestFrequency(-8, 15, 0), new TestFrequency(-4, 7, 0));
        assertMatches("Incorrect values for range [-8,-4]!", correct, fe.frequentBetween((short)-8,(short)-4, true, true));
        correct = Arrays.asList(new TestFrequency(-8, 15, 0));
        assertMatches("Incorrect values for range [-8,-4)!", correct, fe.frequentBetween((short)-8,(short)-4, true, false));
        correct = Arrays.asList(new TestFrequency(-4, 7, 0));
        assertMatches("Incorrect values for range (-8,-4]!", correct, fe.frequentBetween((short)-8,(short)-4, false, true));
        correct = Collections.emptyList();
        assertMatches("Incorrect values for range (-8,-4)!", correct, fe.frequentBetween((short)-8,(short)-4, false, false));

        //check from -4 to 0
        correct = Arrays.asList(new TestFrequency(-4, 7, 0), new TestFrequency(0, 15, 0));
        assertMatches("Incorrect values for range [-4,0]!", correct, fe.frequentBetween((short)-4,(short)0, true, true));
        correct = Arrays.asList(new TestFrequency(-4, 7, 0));
        assertMatches("Incorrect values for range [-4,0)!", correct, fe.frequentBetween((short)-4, (short)0, true, false));
        correct = Arrays.asList(new TestFrequency(0, 15, 0));
        assertMatches("Incorrect values for range (-4,0]!", correct, fe.frequentBetween((short)-4, (short)0, false, true));
        correct = Collections.emptyList();
        assertMatches("Incorrect values for range (-4,0]!", correct, fe.frequentBetween((short)-4, (short)0, false, false));

        //check from 0 to 4
        correct = Arrays.asList(new TestFrequency(4, 7, 0), new TestFrequency(0, 15, 0));
        assertMatches("Incorrect values for range [0,4]!", correct, fe.frequentBetween((short)0, (short)4, true, true));
        correct = Arrays.asList(new TestFrequency(0, 15, 0));
        assertMatches("Incorrect values for range [0,4)!", correct, fe.frequentBetween((short)0, (short)4, true, false));
        correct = Arrays.asList(new TestFrequency(4, 7, 0));
        assertMatches("Incorrect values for range (0,4]!", correct, fe.frequentBetween((short)0, (short)4, false, true));
        correct = Collections.emptyList();
        assertMatches("Incorrect values for range (0,4)!", correct, fe.frequentBetween((short)0, (short)4, false, false));

        //check from 4 to 8
        correct = Arrays.asList(new TestFrequency(4, 7, 0));
        assertMatches("Incorrect values for range [4,8]!", correct, fe.frequentBetween((short)4, (short)8, true, true));
        assertMatches("Incorrect values for range [4,8)!", correct, fe.frequentBetween((short)4, (short)8, true, false));
        correct = Collections.emptyList();
        assertMatches("Incorrect values for range (4,8]!", correct, fe.frequentBetween((short)4, (short)8, false, true));
        assertMatches("Incorrect values for range (4,8)!", correct, fe.frequentBetween((short)4, (short)8, false, false));

        //check from -8 to 0
        correct = Arrays.asList(new TestFrequency(-8, 15, 0), new TestFrequency(-4, 7, 0), new TestFrequency(0, 15, 0));
        assertMatches("Incorrect values for range [-8,0]!", correct, fe.frequentBetween((short)-8, (short)0, true, true));
        correct = Arrays.asList(new TestFrequency(-8, 15, 0), new TestFrequency(-4, 7, 0));
        assertMatches("Incorrect values for range [-8,0)!", correct, fe.frequentBetween((short)-8, (short)0, true, false));
        correct = Arrays.asList(new TestFrequency(0, 15, 0), new TestFrequency(-4, 7, 0));
        assertMatches("Incorrect values for range (-8,0]!", correct, fe.frequentBetween((short)-8, (short)0, false, true));
        correct = Arrays.asList(new TestFrequency(-4, 7, 0));
        assertMatches("Incorrect values for range (-8,0]!", correct, fe.frequentBetween((short)-8, (short)0, false, false));

        //check from 0 to 8
        correct = Arrays.asList(new TestFrequency(4, 7, 0), new TestFrequency(0, 15, 0));
        assertMatches("Incorrect values for range [0,8]!", correct, fe.frequentBetween((short)0, (short)8, true, true));
        assertMatches("Incorrect values for range [0,8)!", correct, fe.frequentBetween((short)0, (short)8, true, false));
        correct = Arrays.asList(new TestFrequency(4, 7, 0));
        assertMatches("Incorrect values for range (0,8]!", correct, fe.frequentBetween((short)0, (short)8, false, true));
        assertMatches("Incorrect values for range (0,8]!", correct, fe.frequentBetween((short)0, (short)8, false, false));
    }

    private void assertMatches(String message, List<TestFrequency> correct,
                               Collection<? extends ShortFrequencyEstimate> actual) {
        Assert.assertEquals(message + ":incorrect size!", correct.size(), actual.size());
        List<ShortFrequencyEstimate> actualEst = Lists.newArrayList(actual);
        Comparator<ShortFrequencyEstimate> c1 = new Comparator<ShortFrequencyEstimate>() {
            @Override
            public int compare(ShortFrequencyEstimate o1, ShortFrequencyEstimate o2) {
                return o1.getValue() - o2.getValue();
            }
        };
        Collections.sort(correct, c1);
        Collections.sort(actualEst, c1);
        for (int i = 0; i < correct.size(); i++) {
            ShortFrequencyEstimate c = correct.get(i);
            ShortFrequencyEstimate a = actualEst.get(i);
            Assert.assertEquals(message + ":Incorrect estimate at pos " + i, c, a);
//            Assert.assertEquals(message+":Incorrect value at pos "+ i,c.getValue(),a.getValue());
//            Assert.assertEquals(message+":Incorrect count at pos "+ i,c.count(),a.count());
//            Assert.assertEquals(message+":Incorrect error at pos "+ i,c.error(),a.error());
        }
    }

    private void assertEmpty(String message, Set<? extends ShortFrequencyEstimate> frequencyEstimates) {
        Assert.assertEquals(message, 0, frequencyEstimates.size());
        Assert.assertFalse(message + ":Iterator claims to hasNext()", frequencyEstimates.iterator().hasNext());
    }

    private void fillPowersOf2(ShortFrequencyCounter counter) {
        for (short i = -8; i < 8; i++) {
            counter.update(i);
            if (i % 2 == 0) counter.update(i, 2);
            if (i % 4 == 0) counter.update(i, 4);
            if (i % 8 == 0) counter.update(i, 8);
        }
    }

    private static class TestFrequency implements ShortFrequencyEstimate {
        final short value;
        long count;
        long error;


        public TestFrequency(int value, long count, long error) {
            this.value = (short)value;
            this.count = count;
            this.error = error;
        }

        public TestFrequency(short value, long count, long error) {
            this.value = value;
            this.count = count;
            this.error = error;
        }

        @Override
        public int compareTo(ShortFrequencyEstimate o) {
            return Shorts.compare(o.value(), value);
        }

        @Override
        public short value() {
            return value;
        }

        @Override
        public Short getValue() {
            return value;
        }

        @Override
        public long count() {
            return count;
        }

        @Override
        public long error() {
            return error;
        }

        @Override
        public FrequencyEstimate<Short> merge(FrequencyEstimate<Short> other) {
            count+=other.count();
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof FrequencyEstimate)) return false;

            @SuppressWarnings("unchecked") FrequencyEstimate<Short> that = (FrequencyEstimate<Short>) o;

            if (value != that.getValue()) return false;
            long guaranteedValue = count - error;
            long otherGV = that.count() - that.error();

            return guaranteedValue == otherGV;
        }

        @Override
        public int hashCode() {
            int result = value;
            result = 31 * result + (int) (count ^ (count >>> 32));
            return result;
        }

        @Override
        public String toString() {
            return "Frequency(" + value + "," + count + ")";
        }
    }
}
