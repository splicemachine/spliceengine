package com.splicemachine.stats.frequency;

import com.google.common.collect.Lists;
import com.google.common.primitives.Ints;
import com.splicemachine.hash.HashFunctions;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

/**
 * @author Scott Fines
 *         Date: 2/18/15
 */
public class IntSpaceSaverTest {

    @Test
    public void testFrequentElementsCorrectWithNoEviction() throws Exception {
        IntFrequencyCounter counter = new IntSpaceSaver(HashFunctions.murmur3(0), 100);

        fillPowersOf2(counter);

        IntFrequentElements fe = counter.frequentElements(2);
        checkMultiplesOf8(fe);
        checkMultiplesOf4(counter.frequentElements(4));
    }

    @Test
    public void testFrequentElementsCorrectWithEviction() throws Exception {
        IntFrequencyCounter counter = new IntSpaceSaver(HashFunctions.murmur3(0), 100);

        fillPowersOf2(counter);

        IntFrequentElements fe = counter.frequentElements(2);
        checkMultiplesOf8(fe);
        checkMultiplesOf4(counter.frequentElements(4));
    }

    private void checkMultiplesOf8(IntFrequentElements fe) {
        Assert.assertEquals("Incorrect value for -8!", 15, fe.equal(-8).count());
        Assert.assertEquals("Incorrect value for 0!", 15, fe.equal(0).count());

        //check before -8
        assertEmpty("Values <-8 found!", fe.frequentBefore(-8, false));
        //check includes -8
        List<TestFrequency> correct = Arrays.asList(new TestFrequency(-8, 15, 0));
        assertMatches("Values <-8 found!", correct, fe.frequentBefore(-8, true));

        //check from -8 to 0
        correct = Arrays.asList(new TestFrequency(-8, 15, 0), new TestFrequency(0, 15, 0));
        assertMatches("Incorrect values for range [-8,0]!", correct, fe.frequentBetween(-8, 0, true, true));
        correct = Arrays.asList(new TestFrequency(-8, 15, 0));
        assertMatches("Incorrect values for range [-8,0)!", correct, fe.frequentBetween(-8, 0, true, false));
        correct = Arrays.asList(new TestFrequency(0, 15, 0));
        assertMatches("Incorrect values for range (-8,0]!", correct, fe.frequentBetween(-8, 0, false, true));
        correct = Collections.emptyList();
        assertMatches("Incorrect values for range (-8,0]!", correct, fe.frequentBetween(-8, 0, false, false));

        //check from 0 to 8
        assertMatches("Values >0 found!", Collections.<TestFrequency>emptyList(), fe.frequentAfter(0, false));
        correct = Arrays.asList(new TestFrequency(0, 15, 0));
        assertMatches("Incorrect values for range [0,8]!", correct, fe.frequentBetween(0, 8, true, true));
        assertMatches("Incorrect values for range [0,8)!", correct, fe.frequentBetween(0, 8, true, false));
        correct = Collections.emptyList();
        assertMatches("Incorrect values for range (0,8]!", correct, fe.frequentBetween(0, 8, false, true));
        assertMatches("Incorrect values for range (0,8)!", correct, fe.frequentBetween(0, 8, false, false));
    }

    private void checkMultiplesOf4(IntFrequentElements fe) {
        Assert.assertEquals("Incorrect value for -8!", 15, fe.equal(-8).count());
        Assert.assertEquals("Incorrect value for -4!", 7, fe.equal(-4).count() - fe.equal(-4).error());
        Assert.assertEquals("Incorrect value for 0!", 15, fe.equal(0).count());
        Assert.assertEquals("Incorrect value for -4!", 7, fe.equal(4).count() - fe.equal(4).error());

        //check before -8
        assertEmpty("Values <-8 found!", fe.frequentBefore(-8, false));
        //check includes -8
        List<TestFrequency> correct = Arrays.asList(new TestFrequency(-8, 15, 0));
        assertMatches("Values <-8 found!", correct, fe.frequentBefore(-8, true));

        //check from -8 to -4
        correct = Arrays.asList(new TestFrequency(-8, 15, 0), new TestFrequency(-4, 7, 0));
        assertMatches("Incorrect values for range [-8,-4]!", correct, fe.frequentBetween(-8, -4, true, true));
        correct = Arrays.asList(new TestFrequency(-8, 15, 0));
        assertMatches("Incorrect values for range [-8,-4)!", correct, fe.frequentBetween(-8, -4, true, false));
        correct = Arrays.asList(new TestFrequency(-4, 7, 0));
        assertMatches("Incorrect values for range (-8,-4]!", correct, fe.frequentBetween(-8, -4, false, true));
        correct = Collections.emptyList();
        assertMatches("Incorrect values for range (-8,-4)!", correct, fe.frequentBetween(-8, -4, false, false));

        //check from -4 to 0
        correct = Arrays.asList(new TestFrequency(-4, 7, 0), new TestFrequency(0, 15, 0));
        assertMatches("Incorrect values for range [-4,0]!", correct, fe.frequentBetween(-4, 0, true, true));
        correct = Arrays.asList(new TestFrequency(-4, 7, 0));
        assertMatches("Incorrect values for range [-4,0)!", correct, fe.frequentBetween(-4, 0, true, false));
        correct = Arrays.asList(new TestFrequency(0, 15, 0));
        assertMatches("Incorrect values for range (-4,0]!", correct, fe.frequentBetween(-4, 0, false, true));
        correct = Collections.emptyList();
        assertMatches("Incorrect values for range (-4,0]!", correct, fe.frequentBetween(-4, 0, false, false));

        //check from 0 to 4
        correct = Arrays.asList(new TestFrequency(4, 7, 0), new TestFrequency(0, 15, 0));
        assertMatches("Incorrect values for range [0,4]!", correct, fe.frequentBetween(0, 4, true, true));
        correct = Arrays.asList(new TestFrequency(0, 15, 0));
        assertMatches("Incorrect values for range [0,4)!", correct, fe.frequentBetween(0, 4, true, false));
        correct = Arrays.asList(new TestFrequency(4, 7, 0));
        assertMatches("Incorrect values for range (0,4]!", correct, fe.frequentBetween(0, 4, false, true));
        correct = Collections.emptyList();
        assertMatches("Incorrect values for range (0,4)!", correct, fe.frequentBetween(0, 4, false, false));

        //check from 4 to 8
        correct = Arrays.asList(new TestFrequency(4, 7, 0));
        assertMatches("Incorrect values for range [4,8]!", correct, fe.frequentBetween(4, 8, true, true));
        assertMatches("Incorrect values for range [4,8)!", correct, fe.frequentBetween(4, 8, true, false));
        correct = Collections.emptyList();
        assertMatches("Incorrect values for range (4,8]!", correct, fe.frequentBetween(4, 8, false, true));
        assertMatches("Incorrect values for range (4,8)!", correct, fe.frequentBetween(4, 8, false, false));

        //check from -8 to 0
        correct = Arrays.asList(new TestFrequency(-8, 15, 0), new TestFrequency(-4, 7, 0), new TestFrequency(0, 15, 0));
        assertMatches("Incorrect values for range [-8,0]!", correct, fe.frequentBetween(-8, 0, true, true));
        correct = Arrays.asList(new TestFrequency(-8, 15, 0), new TestFrequency(-4, 7, 0));
        assertMatches("Incorrect values for range [-8,0)!", correct, fe.frequentBetween(-8, 0, true, false));
        correct = Arrays.asList(new TestFrequency(0, 15, 0), new TestFrequency(-4, 7, 0));
        assertMatches("Incorrect values for range (-8,0]!", correct, fe.frequentBetween(-8, 0, false, true));
        correct = Arrays.asList(new TestFrequency(-4, 7, 0));
        assertMatches("Incorrect values for range (-8,0]!", correct, fe.frequentBetween(-8, 0, false, false));

        //check from 0 to 8
        correct = Arrays.asList(new TestFrequency(4, 7, 0), new TestFrequency(0, 15, 0));
        assertMatches("Incorrect values for range [0,8]!", correct, fe.frequentBetween(0, 8, true, true));
        assertMatches("Incorrect values for range [0,8)!", correct, fe.frequentBetween(0, 8, true, false));
        correct = Arrays.asList(new TestFrequency(4, 7, 0));
        assertMatches("Incorrect values for range (0,8]!", correct, fe.frequentBetween(0, 8, false, true));
        assertMatches("Incorrect values for range (0,8]!", correct, fe.frequentBetween(0, 8, false, false));
    }

    private void assertMatches(String message, List<TestFrequency> correct,
                               Collection<? extends IntFrequencyEstimate> actual) {
        Assert.assertEquals(message + ":incorrect size!", correct.size(), actual.size());
        List<IntFrequencyEstimate> actualEst = Lists.newArrayList(actual);
        Comparator<IntFrequencyEstimate> c1 = new Comparator<IntFrequencyEstimate>() {
            @Override
            public int compare(IntFrequencyEstimate o1, IntFrequencyEstimate o2) {
                return o1.getValue() - o2.getValue();
            }
        };
        Collections.sort(correct, c1);
        Collections.sort(actualEst, c1);
        for (int i = 0; i < correct.size(); i++) {
            IntFrequencyEstimate c = correct.get(i);
            IntFrequencyEstimate a = actualEst.get(i);
            Assert.assertEquals(message + ":Incorrect estimate at pos " + i, c, a);
//            Assert.assertEquals(message+":Incorrect value at pos "+ i,c.getValue(),a.getValue());
//            Assert.assertEquals(message+":Incorrect count at pos "+ i,c.count(),a.count());
//            Assert.assertEquals(message+":Incorrect error at pos "+ i,c.error(),a.error());
        }
    }

    private void assertEmpty(String message, Set<? extends IntFrequencyEstimate> frequencyEstimates) {
        Assert.assertEquals(message, 0, frequencyEstimates.size());
        Assert.assertFalse(message + ":Iterator claims to hasNext()", frequencyEstimates.iterator().hasNext());
    }

    private void fillPowersOf2(IntFrequencyCounter counter) {
        for (int i = -8; i < 8; i++) {
            System.out.println(i);
            counter.update(i);
            if (i % 2 == 0) counter.update(i, 2);
            if (i % 4 == 0) counter.update(i, 4);
            if (i % 8 == 0) counter.update(i, 8);
        }
    }

    private static class TestFrequency implements IntFrequencyEstimate {
        final int value;
        final long count;
        long error;

        public TestFrequency(int value, long count, long error) {
            this.value = value;
            this.count = count;
            this.error = error;
        }

        @Override
        public int compareTo(IntFrequencyEstimate o) {
            return Ints.compare(o.value(), value);
        }

        @Override
        public int value() {
            return value;
        }

        @Override
        public Integer getValue() {
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
        public boolean equals(Object o) {
            if (this == o) return true;
            if (!(o instanceof FrequencyEstimate)) return false;

            @SuppressWarnings("unchecked") FrequencyEstimate<Integer> that = (FrequencyEstimate<Integer>) o;

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
