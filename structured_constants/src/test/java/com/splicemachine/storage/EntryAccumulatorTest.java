package com.splicemachine.storage;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.utils.kryo.KryoPool;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;

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
        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(fields, Collections.<Predicate>emptyList());
        SparseEntryAccumulator accumulator = new SparseEntryAccumulator(predicateFilter,fields);
        accumulator.add(2, ByteBuffer.wrap(Encoding.encode(1)));
        accumulator.add(0, ByteBuffer.wrap(Encoding.encode(2)));

        byte[] bytes = accumulator.finish();
        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(bytes, KryoPool.defaultPool());
        Assert.assertEquals(2,decoder.decodeNextInt());
        Assert.assertEquals(1,decoder.decodeNextInt());
    }

    @Test
    public void testAlwaysAcceptEntryWorks() throws Exception {
        BitSet fields = new BitSet();
        fields.set(0);
        fields.set(2);
        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(fields, Collections.<Predicate>emptyList());
        EntryAccumulator accumulator = new AlwaysAcceptEntryAccumulator(predicateFilter);
        accumulator.add(2, ByteBuffer.wrap(Encoding.encode(1)));
        accumulator.add(0, ByteBuffer.wrap(Encoding.encode(2)));

        byte[] bytes = accumulator.finish();
        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(bytes,KryoPool.defaultPool());
        Assert.assertEquals(2,decoder.decodeNextInt());
        Assert.assertEquals(1,decoder.decodeNextInt());
    }
}
