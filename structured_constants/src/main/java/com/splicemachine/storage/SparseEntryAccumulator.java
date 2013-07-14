package com.splicemachine.storage;

import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;

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
    private final boolean returnIndex;
    private BitSet scalarFields;
    private BitSet floatFields;
    private BitSet doubleFields;

    public SparseEntryAccumulator(BitSet remainingFields) {
        this.allFields = remainingFields;
        this.remainingFields = (BitSet)remainingFields.clone();
        fields = new ByteBuffer[remainingFields.length()];
        this.returnIndex = false;
    }

    public SparseEntryAccumulator(BitSet remainingFields,boolean returnIndex) {
        this.allFields = remainingFields;
        this.remainingFields = (BitSet)remainingFields.clone();
        fields = new ByteBuffer[remainingFields.length()];
        this.returnIndex = returnIndex;
        this.scalarFields = new BitSet();
        this.floatFields = new BitSet();
        this.doubleFields = new BitSet();
    }

    @Override
    public void add(int position, ByteBuffer buffer){
        if(!remainingFields.get(position))
            return; //already populated that field

        fields[position] = buffer;
        buffer.mark(); //assume that we are adding the buffer at the correct position
        remainingFields.clear(position);
    }

    @Override
    public void addScalar(int position, ByteBuffer buffer) {
        add(position,buffer);
        if(returnIndex)
            scalarFields.set(position);
    }

    @Override
    public void addFloat(int position, ByteBuffer buffer) {
        add(position,buffer);
        if(returnIndex)
            floatFields.set(position);
    }

    @Override
    public void addDouble(int position, ByteBuffer buffer) {
        add(position,buffer);
        if(returnIndex)
            doubleFields.set(position);
    }

    @Override
    public BitSet getRemainingFields(){
        return remainingFields;
    }

    @Override
    public byte[] finish(){
        if(returnIndex){
            BitIndex index = BitIndexing.getBestIndex(allFields,scalarFields,floatFields,doubleFields);
            byte[] indexBytes = index.encode();

            byte[] dataBytes = getDataBytes();

            byte[] finalBytes = new byte[indexBytes.length+dataBytes.length+1];
            System.arraycopy(indexBytes,0,finalBytes,0,indexBytes.length);
            System.arraycopy(dataBytes,0,finalBytes,indexBytes.length+1,dataBytes.length);
            return finalBytes;
        }
        return getDataBytes();
    }

    private byte[] getDataBytes() {
        int size=0;
        boolean isFirst=true;
        for(ByteBuffer buffer:fields){
            if(isFirst)isFirst=false;
            else
                size++;

            if(buffer!=null){
                buffer.reset();
                size+=buffer.remaining();
            }
        }

        byte[] bytes = new byte[size];
        int offset=0;
        isFirst=true;
        for(ByteBuffer buffer:fields){
            if(isFirst)isFirst=false;
            else
                offset++;

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
        for(int i=0;i<fields.length;i++){
            fields[i] = null;
        }
    }

    @Override
    public boolean fieldsMatch(EntryAccumulator oldKeyAccumulator) {
        for(int myFields=allFields.nextSetBit(0);myFields>=0;myFields=allFields.nextSetBit(myFields+1)){
            if(!oldKeyAccumulator.hasField(myFields)) return false;
        }
        return true;
    }

    @Override
    public boolean hasField(int myFields) {
        return !remainingFields.get(myFields);
    }
}
