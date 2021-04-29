package com.splicemachine.pipeline.writehandler;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.BitSetIterator;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;
import org.junit.Test;

import java.util.Random;

import static com.splicemachine.pipeline.writehandler.UpdateUtils.halveSet;
import static org.junit.Assert.assertEquals;

public class UpdateUtilsTest {

    @Test
    public void extractFromMutation() throws Exception{
        BitSet bitSet = fromArray(new int[] {0, 2, 4, 6});
        BitIndex index = BitIndexing.getBestIndex(
                bitSet, new BitSet(), new BitSet(), new BitSet());
        byte[] indexBytes = index.encode();
        byte[] values = Bytes.toBytesBinary("AA\\x00BB\\x00CC\\x00DD");
        byte[] result = new byte[indexBytes.length + values.length + 1];

        System.arraycopy(indexBytes, 0, result, 0, indexBytes.length);
        result[indexBytes.length] = 0;
        System.arraycopy(values,0,result,indexBytes.length+1,values.length);

        KVPair kvPair = new KVPair(new byte[] {0x00}, result, KVPair.Type.UPDATE);

        // Check update extraction
        {
            KVPair update = UpdateUtils.getBaseUpdateMutation(kvPair);

            assertEquals(KVPair.Type.UPDATE, update.getType());
            EntryDecoder decoder = new EntryDecoder(update.getValue());
            BitIndex updateIndex = decoder.getCurrentIndex();
            assertEquals(fromArray(new int[]{0, 2}), updateIndex.getFields());
            MultiFieldDecoder fieldDecoder = decoder.getEntryDecoder();
            byte[] value = fieldDecoder.slice(fieldDecoder.length() - fieldDecoder.offset());
            assertEquals("AA\\x00BB", Bytes.toStringBinary(value));
        }


        // Check delete extraction
        {
            KVPair delete = UpdateUtils.deleteFromUpdate(kvPair);

            assertEquals(KVPair.Type.DELETE, delete.getType());
            EntryDecoder decoder = new EntryDecoder(delete.getValue());
            BitIndex updateIndex = decoder.getCurrentIndex();
            assertEquals(fromArray(new int[]{0, 2}), updateIndex.getFields());
            MultiFieldDecoder fieldDecoder = decoder.getEntryDecoder();
            byte[] value = fieldDecoder.slice(fieldDecoder.length() - fieldDecoder.offset());
            assertEquals("CC\\x00DD", Bytes.toStringBinary(value));
        }
    }

    @Test
    public void testHalveSet() throws Exception{
        Random random = new Random();
        for(int c = 0; c < 1000; ++c) {
            int size = random.nextInt(10000);
            BitSet bitSet = new BitSet(size);
            for (int i = 0; i < size; ++i) {
                if(random.nextBoolean())
                    bitSet.set(i);
            }

            BitSet dupSet = duplicateBitSet(bitSet, size);
            assertEquals("Failed for size " + size + " and BitSet " + bitSet, bitSet, halveSet(dupSet));
        }

        assertEquals(fromArray(new int[]{0, 2}), halveSet(fromArray(new int[] {0, 2, 4, 6})));
        assertEquals(fromArray(new int[]{1, 3, 6}), halveSet(fromArray(new int[] {1, 3, 6, 8, 9, 10})));
        assertEquals(fromArray(new int[]{0}), halveSet(fromArray(new int[] {0, 2})));
        assertEquals(fromArray(new int[]{}), halveSet(fromArray(new int[] {})));
    }

    BitSet fromArray(int[] cols) {
        BitSet result = new BitSet();
        for (int i : cols) {
            result.set(i);
        }
        return result;
    }

    private BitSet duplicateBitSet(BitSet bitSet, int size) {
        BitSet result = (BitSet) bitSet.clone();
        BitSetIterator it = bitSet.iterator();
        for(int i = it.nextSetBit(); i != -1; i = it.nextSetBit()) {
            result.set(i + size);
        }
        return result;
    }
}
