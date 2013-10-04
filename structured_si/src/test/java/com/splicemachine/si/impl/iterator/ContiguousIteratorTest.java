package com.splicemachine.si.impl.iterator;

import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Test;

import java.util.Comparator;
import java.util.Iterator;

public class ContiguousIteratorTest {
    @Test
    public void singleGap() {
        check(0, new Integer[]{0, 1, 2, 4, 5},
                new Integer[]{0, 1, 2, -3, 4, 5});
    }

    @Test
    public void mutliGap() {
        check(0, new Integer[]{0, 2, 4, 5},
                new Integer[]{0, -1, 2, -3, 4, 5});
    }

    @Test
    public void longGap() {
        check(0, new Integer[]{0, 4, 5},
                new Integer[]{0, -1, -2, -3, 4, 5});
    }

    @Test
    public void emptyInput() {
        check(0, new Integer[]{},
                new Integer[]{});
    }

    @Test
    public void differentStart() {
        check(2, new Integer[]{2, 4, 5},
                new Integer[]{2, -3, 4, 5});
    }

    @Test
    public void startBeforeFirst() {
        check(1, new Integer[]{3, 5},
                new Integer[]{-1, -2, 3, -4, 5});
    }

    @Test
    public void startBeyondFirst() {
        try {
            check(2, new Integer[]{0, 1, 2, 4, 5},
                new Integer[]{0, 1, 2, -3, 4, 5});
            Assert.fail();
        } catch (RuntimeException ex) {
            Assert.assertEquals("expected value is ahead of actual value", ex.getMessage());
        }
    }

    @Test
    public void withMuxer() {
        final Iterator<Integer> muxer = new OrderedMuxer(OrderedMuxerTest.setupSources(new Integer[][]{
                {0, 2, 4},
                {1, 5, 9}}), comparator, decoder);
        final Iterator<Integer> result = new ContiguousIterator<Integer, Integer>(0, muxer, comparator, decoder, callbacks);
        Assert.assertArrayEquals(new Integer[]{0, 1, 2, -3, 4, 5, -6, -7, -8, 9}, OrderedMuxerTest.resultToArray(result));
    }

    private void check(int start, Integer[] expected, Integer[] input) {
        final Iterator<Integer> result = new ContiguousIterator(start, Iterators.<Integer>forArray(expected), comparator, decoder, callbacks);
        Assert.assertArrayEquals(input, OrderedMuxerTest.resultToArray(result));
    }

    final static Comparator<Integer> comparator = new Comparator<Integer>() {
        @Override
        public int compare(Integer o1, Integer o2) {
            if (o1 < o2) {
                return -1;
            } else if (o1 > o2) {
                return 1;
            } else {
                return 0;
            }
        }
    };

    final static DataIDDecoder<Integer, Integer> decoder = new DataIDDecoder<Integer, Integer>() {
        @Override
        public Integer getID(Integer integer) {
            return integer;
        }
    };

    final static ContiguousIteratorFunctions<Integer, Integer> callbacks = new ContiguousIteratorFunctions<Integer, Integer>() {
        @Override
        public Integer increment(Integer integer) {
            return integer + 1;
        }

        @Override
        public Integer missing(Integer integer) {
            return integer * -1;
        }
    };
}
