/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
