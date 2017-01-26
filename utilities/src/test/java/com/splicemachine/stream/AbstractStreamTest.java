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

package com.splicemachine.stream;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class AbstractStreamTest {

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

    private static class IntegerStream extends AbstractStream<Integer> {
        int i = 0;

        @Override
        public Integer next() throws StreamException {
            i++;
            return i < 10 ? i : null;
        }

        @Override public void close() throws StreamException {  }
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
