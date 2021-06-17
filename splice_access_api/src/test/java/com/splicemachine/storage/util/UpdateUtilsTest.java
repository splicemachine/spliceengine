/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

package com.splicemachine.storage.util;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.BitSetIterator;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.Random;

import static com.splicemachine.storage.util.UpdateUtils.halveSet;
import static com.splicemachine.storage.util.UpdateUtils.mergeUpdate;
import static org.junit.Assert.assertEquals;

public class UpdateUtilsTest {

    @Test
    public void extractFromMutation() {
        KVPair kvPair = kvPair(new int[] {0, 2, 4, 6}, Bytes.toBytesBinary("AA\\x00BB\\x00CC\\x00DD"));

        // Check update extraction
        KVPair update = UpdateUtils.updateFromWrite(kvPair);

        assertEquals(KVPair.Type.UPDATE, update.getType());
        assertEquals(fromArray(new int[]{0, 2}), getFields(update));
        assertEquals("AA\\x00BB", getStringBinaryValue(update));


        // Check delete extraction
        KVPair delete = UpdateUtils.deleteFromWrite(kvPair);

        assertEquals(KVPair.Type.DELETE, delete.getType());
        assertEquals(fromArray(new int[]{0, 2}), getFields(delete));
        assertEquals("CC\\x00DD", getStringBinaryValue(delete));
    }

    @Test
    public void testHalveSet() {
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

    @Test
    public void testMergeUpdate() throws IOException {
        KVPair update = kvPair(new int[] {0, 2, 4, 6},
                Bytes.toBytesBinary("AA\\x00BB\\x00CC\\x00DD"));
        KVPair dataKV = kvPair(new int[] {0, 1, 2, 3, 4, 5, 6},
                Bytes.toBytesBinary("00\\x0011\\x0022\\x0033\\x0044\\x0055\\x0066"));

        mergeUpdate(update, mockCell(dataKV.getValue()));

        assertEquals(fromArray(new int[] {0, 1, 2, 3, 4, 5, 6}), getFields(update));
        assertEquals("AA\\x0011\\x00BB\\x0033\\x00CC\\x0055\\x00DD",
                getStringBinaryValue(update));
    }


    @Test
    public void testMergeUpdateWithNull() throws IOException {
        KVPair update = kvPair(new int[] {0, 2, 4, 6},
                Bytes.toBytesBinary("AA\\x00\\x00CC\\x00DD")); // 2 is NULL
        KVPair dataKV = kvPair(new int[] {0, 1, 2, 3, 4, 5, 6},
                Bytes.toBytesBinary("00\\x0011\\x0022\\x0033\\x0044\\x0055\\x0066"));

        mergeUpdate(update, mockCell(dataKV.getValue()));

        assertEquals(fromArray(new int[] {0, 1, 2, 3, 4, 5, 6}), getFields(update));
        assertEquals("AA\\x0011\\x00\\x0033\\x00CC\\x0055\\x00DD",
                getStringBinaryValue(update));
    }

    @Test
    public void testMergeUpdateWithNullAtEnd() throws IOException {
        KVPair update = kvPair(new int[] {0, 2, 4, 6},
                Bytes.toBytesBinary("AA\\x00BB\\x00CC\\x00")); // 6 is NULL
        KVPair dataKV = kvPair(new int[] {0, 1, 2, 3, 4, 5, 6},
                Bytes.toBytesBinary("00\\x0011\\x0022\\x0033\\x0044\\x0055\\x0066"));

        mergeUpdate(update, mockCell(dataKV.getValue()));

        assertEquals(fromArray(new int[] {0, 1, 2, 3, 4, 5, 6}), getFields(update));
        assertEquals("AA\\x0011\\x00BB\\x0033\\x00CC\\x0055\\x00",
                getStringBinaryValue(update));
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

    private DataCell mockCell(byte[] data) {
        DataCell cell = Mockito.mock(DataCell.class);
        Mockito.when(cell.valueArray()).thenReturn(data);
        Mockito.when(cell.valueOffset()).thenReturn(0);
        Mockito.when(cell.valueLength()).thenReturn(data.length);
        return cell;
    }

    private KVPair kvPair(int[] fields, byte[] values) {
        BitSet bitSet = fromArray(fields);
        BitIndex index = BitIndexing.getBestIndex(bitSet, new BitSet(), new BitSet(), new BitSet());
        byte[] indexBytes = index.encode();
        byte[] result = new byte[indexBytes.length + values.length + 1];

        System.arraycopy(indexBytes, 0, result, 0, indexBytes.length);
        result[indexBytes.length] = 0;
        System.arraycopy(values,0,result,indexBytes.length+1,values.length);

        return new KVPair(new byte[] {0x00}, result, KVPair.Type.UPDATE);
    }


    // Returns the bitset of fields this value has
    private BitSet getFields(KVPair pair) {
        EntryDecoder decoder = new EntryDecoder(pair.getValue());
        BitIndex updateIndex = decoder.getCurrentIndex();
        return updateIndex.getFields();
    }

    // Returns the actual encoded valued
    private String getStringBinaryValue(KVPair pair) {
        EntryDecoder decoder = new EntryDecoder(pair.getValue());
        MultiFieldDecoder fieldDecoder = decoder.getEntryDecoder();
        return Bytes.toStringBinary(fieldDecoder.slice(fieldDecoder.length() - fieldDecoder.offset()));
    }
}

