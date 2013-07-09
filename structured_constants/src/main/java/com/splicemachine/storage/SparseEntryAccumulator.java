package com.splicemachine.storage;

import com.splicemachine.constants.bytes.BytesUtil;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * @author Scott Fines
 * Created on: 7/8/13
 */
public class SparseEntryAccumulator implements EntryAccumulator {
    private BitSet remainingFields;
    private BitSet allFields;

    public ByteBuffer[] fields;

    SparseEntryAccumulator(BitSet remainingFields) {
        this.allFields = remainingFields;
        this.remainingFields = (BitSet)remainingFields.clone();
        fields = new ByteBuffer[remainingFields.cardinality()];
    }

    @Override
    public void add(int position, ByteBuffer buffer){
        if(!remainingFields.get(position))
            return; //already populated that field

        fields[position] = buffer;
        remainingFields.clear(position);
    }

    @Override
    public BitSet getRemainingFields(){
        return remainingFields;
    }

    @Override
    public byte[] finish(){
        return BytesUtil.concatenate(fields);
    }

    @Override
    public void reset(){
        remainingFields = (BitSet)allFields.clone();
    }

}
