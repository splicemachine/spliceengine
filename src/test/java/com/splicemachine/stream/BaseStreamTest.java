package com.splicemachine.stream;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class BaseStreamTest {

    @Test
    public void forEach() throws StreamException {
        SumAccumulator accumulator = new SumAccumulator();
        new IntegerStream().forEach(accumulator);
        assertEquals(45, accumulator.sum);
    }

    @Test
    public void transform() throws StreamException {
        SumAccumulator accumulator = new SumAccumulator();
        new IntegerStream().transform(new NegateTransformer()).forEach(accumulator);
        assertEquals(-45, accumulator.sum);
    }

    private static class IntegerStream extends BaseStream<Integer> {
        int i = 0;

        @Override
        public Integer next() throws StreamException {
            i++;
            return i < 10 ? i : null;
        }
    }

    private static class SumAccumulator implements Accumulator<Integer> {
        int sum;

        @Override
        public void accumulate(Integer next) throws StreamException {
            sum += next;
        }
    }

    private static class NegateTransformer implements Transformer<Integer, Integer> {
        @Override
        public Integer transform(Integer element) throws StreamException {
            return element * -1;
        }
    }

}
