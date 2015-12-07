package com.splicemachine.storage;

import com.carrotsearch.hppc.ObjectArrayList;
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
        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(fields, new ObjectArrayList<Predicate>());
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
        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(fields, new ObjectArrayList<Predicate>());
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
