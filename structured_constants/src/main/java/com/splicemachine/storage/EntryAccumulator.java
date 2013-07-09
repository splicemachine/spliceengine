package com.splicemachine.storage;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * @author Scott Fines
 * Created on: 7/8/13
 */
public class EntryAccumulator {
    private BitSet remainingFields;
    private BitSet allFields;

    public ByteBuffer[] fields;

    EntryAccumulator(BitSet remainingFields) {
        this.allFields = remainingFields;
        this.remainingFields = (BitSet)remainingFields.clone();
        fields = new ByteBuffer[remainingFields.cardinality()];
    }

    public void add(int position,ByteBuffer buffer){
        if(!remainingFields.get(position))
            return; //already populated that field

        fields[position] = buffer;
        remainingFields.clear(position);
    }

    public BitSet getRemainingFields(){
        return remainingFields;
    }

    public byte[] finish(){
        int size = 0;
        boolean isFirst=true;
        for(int i=0;i<fields.length;i++){
            if(isFirst)isFirst=false;
            else
                size++;

            ByteBuffer buffer = fields[i];
            if(buffer!=null)
                size+=buffer.remaining();
        }

        byte[] bytes = new byte[size];
        int offset=0;
        isFirst=true;
        for(int i=0;i<fields.length;i++){
            if(isFirst)isFirst=false;
            else{
                bytes[offset] = 0x00;
                offset++;
            }
            ByteBuffer buffer = fields[i];
            if(buffer!=null){
                buffer.get(bytes,offset,buffer.remaining());
            }
        }
        return bytes;
    }

    public void reset(){
        remainingFields = (BitSet)allFields.clone();
    }

}
