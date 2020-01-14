/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
