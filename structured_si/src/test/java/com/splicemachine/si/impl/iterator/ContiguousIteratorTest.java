package com.splicemachine.si.impl.iterator;

import com.google.common.collect.Iterators;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ContiguousIteratorTest {
    @Test
    public void singleGap() throws IOException {
        check(0, new Integer[]{0, 1, 2, 4, 5},
                new Integer[]{0, 1, 2, -3, 4, 5});
    }

    @Test
    public void mutliGap() throws IOException {
        check(0, new Integer[]{0, 2, 4, 5},
                new Integer[]{0, -1, 2, -3, 4, 5});
    }

    @Test
    public void longGap() throws IOException {
        check(0, new Integer[]{0, 4, 5},
                new Integer[]{0, -1, -2, -3, 4, 5});
    }

    @Test
    public void emptyInput() throws IOException {
        check(0, new Integer[]{},
                new Integer[]{});
    }

    @Test
    public void differentStart() throws IOException {
        check(2, new Integer[]{2, 4, 5},
                new Integer[]{2, -3, 4, 5});
    }

    @Test
    public void startBeforeFirst() throws IOException {
        check(1, new Integer[]{3, 5},
                new Integer[]{-1, -2, 3, -4, 5});
    }

    @Test
    public void startBeyondFirst() throws IOException {
        try {
            check(2, new Integer[]{0, 1, 2, 4, 5},
                new Integer[]{0, 1, 2, -3, 4, 5});
            Assert.fail();
        } catch (RuntimeException ex) {
            Assert.assertEquals("expected value 2 is ahead of actual value 0", ex.getMessage());
        }
    }

    @Test
    public void withMuxer() throws IOException {
        final Iterator<Integer> muxer = new OrderedMuxer(OrderedMuxerTest.setupSources(new Integer[][]{
                {0, 2, 4},
                {1, 5, 9}}), decoder);
        final ContiguousIterator<Integer, Integer> result = new ContiguousIterator<Integer, Integer>(0, muxer, decoder, callbacks);
        Assert.assertArrayEquals(new Integer[]{0, 1, 2, -3, 4, 5, -6, -7, -8, 9}, resultToArray(result));
    }

    private void check(int start, Integer[] expected, Integer[] input) throws IOException {
        final ContiguousIterator<Integer, Integer> result = new ContiguousIterator(start, Iterators.<Integer>forArray(expected), decoder, callbacks);
        Assert.assertArrayEquals(input, resultToArray(result));
    }

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

    static Integer[] resultToArray(ContiguousIterator<Integer, Integer> sequence) throws IOException {
        List<Integer> result = new ArrayList<Integer>();
        while (sequence.hasNext()) {
            result.add(sequence.next());
        }
        return result.toArray(new Integer[result.size()]);
    }


}
