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

package com.splicemachine.storage.index;

import org.junit.Test;
import com.carrotsearch.hppc.BitSet;
import static com.splicemachine.utils.BitSets.newBitSet;
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

}