package com.splicemachine.pipeline.writehandler;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.BitSetIterator;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;
import com.splicemachine.utils.ByteSlice;

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

    public static KVPair deleteFromUpdate(KVPair mutation) {
        return extractWriteFromUpdate(mutation, KVPair.Type.DELETE);
    }

    public static KVPair getBaseUpdateMutation(KVPair mutation) {
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
}
