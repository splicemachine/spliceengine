package com.splicemachine.storage.index;

import com.carrotsearch.hppc.BitSet;
import org.junit.Test;

import static org.junit.Assert.*;

public class UncompressedBitIndexTest {

    @Test
    public void testDecodedBitIndexEqualsOriginal() {

        BitSet bitSet = newBitSet(0, 1, 2, 3, 4, 5);
        BitSet scalarFields = newBitSet(0, 1);
        BitSet floatFields = newBitSet(2, 3);
        BitSet doubleFields = newBitSet(4, 5);

        BitIndex bitIndex = UncompressedBitIndex.create(bitSet, scalarFields, floatFields, doubleFields);
        assertEquals("{{0, 1, 2, 3, 4, 5},{0, 1},{2, 3},{4, 5}}", bitIndex.toString());

        byte[] data = bitIndex.encode();
        assertArrayEquals(new byte[]{-113, -5, -83}, bitIndex.encode());

        BitIndex decoded = UncompressedBitIndex.wrap(data, 0, data.length);
        assertEquals("{{0, 1, 2, 3, 4, 5},{0, 1},{2, 3},{4, 5}}", decoded.toString());

        assertEquals(bitIndex, decoded);
    }

    /* Looks like decoded UncompressedBitIndexes do not contain type information for unset fields/bits. */
    @Test
    public void testFieldTypeIsNOTPreservedWhenEncodingAndDecoding() {

        BitSet bitSet = newBitSet(0, 2, 4);
        BitSet scalarFields = newBitSet(0, 1);
        BitSet floatFields = newBitSet(2, 3);
        BitSet doubleFields = newBitSet(4, 5);

        BitIndex bitIndex = UncompressedBitIndex.create(bitSet, scalarFields, floatFields, doubleFields);
        assertEquals("{{0, 2, 4},{0, 1},{2, 3},{4, 5}}", bitIndex.toString());

        byte[] data = bitIndex.encode();
        assertArrayEquals(new byte[]{-114, -27}, bitIndex.encode());

        BitIndex decodedBitIndex = UncompressedBitIndex.wrap(data, 0, data.length);
        assertEquals("{{0, 2, 4},{0},{2},{4}}", decodedBitIndex.toString());

        assertNotEquals(bitIndex, decodedBitIndex);
    }

    private BitSet newBitSet(int... bits) {
        BitSet bitSet = new BitSet();
        for (int b : bits) {
            bitSet.set(b);
        }
        return bitSet;
    }

}