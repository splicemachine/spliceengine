package com.splicemachine.storage;

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
        int size = 0;
        boolean isFirst=true;
        for (ByteBuffer field : fields) {
            if (isFirst) isFirst = false;
            else
                size++;

            if (field != null)
                size += field.remaining();
        }

        byte[] bytes = new byte[size];
        int offset=0;
        isFirst=true;
        for (ByteBuffer field : fields) {
            if (isFirst) isFirst = false;
            else {
                bytes[offset] = 0x00;
                offset++;
            }
            if (field != null) {
                field.get(bytes, offset, field.remaining());
            }
        }
        return bytes;
    }

    @Override
    public void reset(){
        remainingFields = (BitSet)allFields.clone();
    }

}
