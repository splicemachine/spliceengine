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
import com.splicemachine.storage.ByteEntryAccumulator;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;
import com.splicemachine.utils.ByteSlice;

import java.io.IOException;

public class UpdateUtils {

    /* Writes are encoded like this: BIT_INDEX 0x00 VALUE1 0x00 VALUE2 0x00 ...
     * For Updates we are now including the new and the old values, for a table with 4 columns were we update
     * columns {0, 2} the BIT_INDEX will be {0,2,4,6}, new values go first and old values last
     *
     * We use the new values to generate the updated index and the old values to generate the delete tombstone. In
     * both cases the BIT_INDEX will be the same (the first half).
     *
     * If we are updating { HI, 5 } to { BYE, 6 } we start with this row:
     *      {0,2,4,6} 0x00 BYE 0x00 6 0x00 HI 0x00 5
     * And get these rows:
     *      delete: {0,2} 0x00 HI 0x00 5
     *      update: {0,2} 0x00 BYE 0x00 6
     */
    private static KVPair extractWriteFromUpdate(KVPair mutation, KVPair.Type type) {
        EntryDecoder rowDecoder = new EntryDecoder();
        rowDecoder.set(mutation.getValue());
        BitIndex index = rowDecoder.getCurrentIndex();
        MultiFieldDecoder fieldDecoder = rowDecoder.getEntryDecoder();
        int cardinality = index.cardinality();
        int valueStart = fieldDecoder.offset();
        for(int i=index.nextSetBit(0), c=0; c<cardinality/2; i=index.nextSetBit(i+1), c++){
            rowDecoder.seekForward(fieldDecoder, i);
        }
        byte[] data;
        if (type == KVPair.Type.DELETE) {
            // get the second half of the value
            data = fieldDecoder.slice(fieldDecoder.length() - fieldDecoder.offset());
        } else { // UPDATE
            // get the first half
            int valueHalf = fieldDecoder.offset();
            fieldDecoder.seek(valueStart);
            data = fieldDecoder.slice(valueHalf - valueStart - 1); // -1 to remove the NULL separator
        }
        BitIndex resultIndex = BitIndexing.getBestIndex(
                halveSet(index.getFields()),
                halveSet(index.getScalarFields()),
                halveSet(index.getFloatFields()),
                halveSet(index.getDoubleFields()));
        byte[] bitData = resultIndex.encode();
        byte[] result = new byte[bitData.length+data.length+1];
        System.arraycopy(bitData, 0, result, 0, bitData.length);
        result[bitData.length] = 0;
        System.arraycopy(data,0,result,bitData.length+1,data.length);

        ByteSlice rowSlice = mutation.rowKeySlice();
        return new KVPair(
                rowSlice.array(),rowSlice.offset(),rowSlice.length(),
                result,0,result.length,type);
    }

    public static KVPair deleteFromWrite(KVPair mutation) {
        return extractWriteFromUpdate(mutation, KVPair.Type.DELETE);
    }

    public static KVPair updateFromWrite(KVPair mutation) {
        return extractWriteFromUpdate(mutation, KVPair.Type.UPDATE);
    }

    /**
     * Returns the first half of a bit set
     */
    public static BitSet halveSet(BitSet bitSet) {
        BitSet result = new BitSet(bitSet.capacity()/2);
        long cardinality = bitSet.cardinality();
        BitSetIterator it = bitSet.iterator();
        int pos;
        long count = 0;
        while((pos = it.nextSetBit()) != -1) {
            result.set(pos);
            count++;
            if (count >= cardinality / 2)
                break;
        }
        return result;
    }

    public static void mergeUpdate(KVPair update, DataCell data) throws IOException {
        if (update.getType() != KVPair.Type.UPDATE && update.getType() != KVPair.Type.UPSERT)
            return;

        EntryDecoder dataDecoder = new EntryDecoder(data.valueArray(), data.valueOffset(), data.valueLength());
        BitIndex dataIndex = dataDecoder.getCurrentIndex();
        ByteSlice updateSlice = update.valueSlice();
        EntryDecoder updateDecoder = new EntryDecoder(updateSlice.array(), updateSlice.offset(), updateSlice.length());
        BitIndex updateIndex = updateDecoder.getCurrentIndex();
        com.carrotsearch.hppc.BitSet fields = (com.carrotsearch.hppc.BitSet) dataIndex.getFields().clone();
        fields.remove(updateIndex.getFields());
        if(fields.isEmpty())
            return;

        fields.union(updateIndex.getFields());
        com.carrotsearch.hppc.BitSet scalarFields = (com.carrotsearch.hppc.BitSet) dataIndex.getScalarFields().clone();
        scalarFields.union(updateIndex.getScalarFields());
        com.carrotsearch.hppc.BitSet floatFields = (com.carrotsearch.hppc.BitSet) dataIndex.getFloatFields().clone();
        floatFields.union(updateIndex.getFloatFields());
        com.carrotsearch.hppc.BitSet doubleFields = (com.carrotsearch.hppc.BitSet) dataIndex.getDoubleFields().clone();
        doubleFields.union(updateIndex.getDoubleFields());
        MultiFieldDecoder updateFieldDecoder = updateDecoder.getEntryDecoder();
        MultiFieldDecoder dataFieldDecoder = dataDecoder.getEntryDecoder();
        ByteEntryAccumulator accumulator = new ByteEntryAccumulator(null, false, fields);
        for (int i = fields.nextSetBit(0); i >= 0; i = fields.nextSetBit(i+1)) {
            MultiFieldDecoder toUse, toAdvance = null;
            if(updateIndex.getFields().get(i)) {
                toUse = updateFieldDecoder;
                if (dataIndex.getFields().get(i)) {
                    toAdvance = dataFieldDecoder;
                }
            } else {
                toUse = dataFieldDecoder;
            }
            int start = toUse.offset();
            if (scalarFields.get(i)) {
                toUse.skipLong();
                if (toAdvance != null) toAdvance.skipLong();
            } else if (floatFields.get(i)) {
                toUse.skipFloat();
                if (toAdvance != null) toAdvance.skipFloat();
            } else if (doubleFields.get(i)) {
                toUse.skipDouble();
                if (toAdvance != null) toAdvance.skipDouble();
            } else {
                toUse.skip();
                if (toAdvance != null) toAdvance.skip();
            }
            int end = toUse.offset();
            int length = end - start - 1; // -1  to remove field separator
            if (length < 0) length = 0; // NULL field
            accumulator.add(i, toUse.array(), start, length);
        }
        BitIndex index = BitIndexing.getBestIndex(fields,scalarFields,floatFields,doubleFields);
        byte[] indexBytes = index.encode();
        byte[] dataBytes = accumulator.finish();

        byte[] finalBytes = new byte[indexBytes.length+dataBytes.length+1];
        System.arraycopy(indexBytes,0,finalBytes,0,indexBytes.length);
        System.arraycopy(dataBytes,0,finalBytes,indexBytes.length+1,dataBytes.length);
        update.setValue(finalBytes);
    }
}
