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

    public SparseEntryAccumulator(BitSet remainingFields) {
        this.allFields = remainingFields;
        this.remainingFields = (BitSet)remainingFields.clone();
        fields = new ByteBuffer[remainingFields.length()];
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
        int size=0;
        boolean isFirst=true;
        for(int i=allFields.nextSetBit(0);i>=0;i=allFields.nextSetBit(i+1)){
            if(isFirst)isFirst=false;
            else
                size++;

            ByteBuffer buffer = fields[i];
            if(buffer!=null){
                size+=buffer.remaining();
            }
        }

        byte[] bytes = new byte[size];
        int offset=0;
        isFirst=true;
        for(int i=allFields.nextSetBit(0);i>=0;i=allFields.nextSetBit(i+1)){
            if(isFirst)isFirst=false;
            else
                offset++;

            ByteBuffer buffer = fields[i];
            if(buffer!=null){
                int newOffset = offset+buffer.remaining();
                buffer.get(bytes,offset,buffer.remaining());
                offset=newOffset;
            }
        }
        return bytes;
    }

    @Override
    public void reset(){
        remainingFields = (BitSet)allFields.clone();
    }

}
