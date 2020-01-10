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
