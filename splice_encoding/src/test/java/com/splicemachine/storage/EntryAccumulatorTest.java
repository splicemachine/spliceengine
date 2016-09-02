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

package com.splicemachine.storage;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import org.junit.Assert;
import org.junit.Test;
import com.carrotsearch.hppc.BitSet;

/**
 * @author Scott Fines
 *         Created on: 7/9/13
 */
public class EntryAccumulatorTest {
    @Test
    public void testSparseEntryWorks() throws Exception {
        BitSet fields = new BitSet();
        fields.set(0);
        fields.set(2);
        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(fields);
        EntryAccumulator accumulator = new ByteEntryAccumulator(predicateFilter,false,fields);
				byte[] encodedOne = Encoding.encode(1);
				accumulator.add(2, encodedOne,0,encodedOne.length);
				byte[] encodedTwo = Encoding.encode(2);
				accumulator.add(0, encodedTwo,0,encodedTwo.length);

        byte[] bytes = accumulator.finish();
        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(bytes);
        Assert.assertEquals(2,decoder.decodeNextInt());
        Assert.assertEquals(1,decoder.decodeNextInt());
    }

    @Test
    public void testAlwaysAcceptEntryWorks() throws Exception {
        BitSet fields = new BitSet();
        fields.set(0);
        fields.set(2);
        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(fields);
        EntryAccumulator accumulator = new ByteEntryAccumulator(predicateFilter,false,null);
				byte[] encodedOne = Encoding.encode(1);
				accumulator.add(2, encodedOne,0,encodedOne.length);
				byte[] encodedTwo = Encoding.encode(2);
				accumulator.add(0, encodedTwo,0,encodedTwo.length);

        byte[] bytes = accumulator.finish();
        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(bytes);
        Assert.assertEquals(2,decoder.decodeNextInt());
        Assert.assertEquals(1,decoder.decodeNextInt());
    }
}
