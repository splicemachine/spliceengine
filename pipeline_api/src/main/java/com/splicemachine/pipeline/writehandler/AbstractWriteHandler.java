package com.splicemachine.pipeline.writehandler;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.BitSetIterator;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;
import com.splicemachine.utils.ByteSlice;

public abstract class AbstractWriteHandler implements WriteHandler {

    protected KVPair getBaseUpdateMutation(KVPair mutation) {
        EntryDecoder rowDecoder = new EntryDecoder();
        rowDecoder.set(mutation.getValue());
        BitIndex index = rowDecoder.getCurrentIndex();
        MultiFieldDecoder fieldDecoder = rowDecoder.getEntryDecoder();
        int offset = fieldDecoder.offset();
        for(int i=index.nextSetBit(0), c=0; c<index.cardinality()/2; i=index.nextSetBit(i+1), c++){
            rowDecoder.seekForward(fieldDecoder, i);
        }
        int end = fieldDecoder.offset();
        fieldDecoder.seek(offset);
        byte[] data = fieldDecoder.slice(end - offset - 1); // -1 to remove the NULL separator
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
                result,0,result.length,KVPair.Type.UPDATE);
    }

    protected BitSet halveSet(BitSet bitSet) {
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
