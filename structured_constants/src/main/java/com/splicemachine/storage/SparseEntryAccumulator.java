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

    private final EntryPredicateFilter predicateFilter;

    public SparseEntryAccumulator(EntryPredicateFilter predicateFilter,BitSet remainingFields) {
        this.allFields = remainingFields;
        this.remainingFields = (BitSet)remainingFields.clone();
        fields = new ByteBuffer[remainingFields.length()];
        this.returnIndex = false;
        this.predicateFilter = predicateFilter;
    }

    public SparseEntryAccumulator(EntryPredicateFilter predicateFilter,BitSet remainingFields,boolean returnIndex) {
        this.allFields = remainingFields;
        this.remainingFields = (BitSet)remainingFields.clone();
        fields = new ByteBuffer[remainingFields.length()];
        this.returnIndex = returnIndex;
        this.scalarFields = new BitSet();
        this.floatFields = new BitSet();
        this.doubleFields = new BitSet();
        this.predicateFilter = predicateFilter;
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
        if(predicateFilter!=null){
            BitSet checkColumns = predicateFilter.getCheckedColumns();
            for(int i=0;i<checkColumns.length();i++){
                if(i>=fields.length||fields[i]==null){
                    if(!predicateFilter.checkPredicates(null,i)) return null;
                }else{
                    ByteBuffer buffer = fields[i];
                    buffer.reset();
                    if(!predicateFilter.checkPredicates(buffer,i)) return null;
                }
            }
        }

        //count up the number of rows visited
        if(predicateFilter!=null)predicateFilter.rowReturned();

        byte[] dataBytes = getDataBytes();
        if(returnIndex){
            BitIndex index = BitIndexing.getBestIndex(allFields,scalarFields,floatFields,doubleFields);
            byte[] indexBytes = index.encode();


            byte[] finalBytes = new byte[indexBytes.length+dataBytes.length+1];
            System.arraycopy(indexBytes,0,finalBytes,0,indexBytes.length);
            System.arraycopy(dataBytes,0,finalBytes,indexBytes.length+1,dataBytes.length);
            return finalBytes;
        }
        return dataBytes;
    }

    private byte[] getDataBytes() {
        int size=0;
        boolean isFirst=true;
        for(int n = allFields.nextSetBit(0);n>=0;n=allFields.nextSetBit(n+1)){
            if(isFirst)isFirst=false;
            else
                size++;
            ByteBuffer buffer = fields[n];
            if(buffer!=null){
                buffer.reset();
                size+=buffer.remaining();
            }
        }

        byte[] bytes = new byte[size];
        int offset=0;
        isFirst=true;
        for(int n=allFields.nextSetBit(0);n>=0;n=allFields.nextSetBit(n+1)){
            if(isFirst)isFirst=false;
            else
                offset++;

            ByteBuffer buffer = fields[n];
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
        if(predicateFilter!=null)
            predicateFilter.reset();
    }

    @Override
    public boolean fieldsMatch(EntryAccumulator oldKeyAccumulator) {
        for(int myFields=allFields.nextSetBit(0);myFields>=0;myFields=allFields.nextSetBit(myFields+1)){
            if(!oldKeyAccumulator.hasField(myFields)) return false;
            ByteBuffer field = fields[myFields];
            if(field==null){
                if(oldKeyAccumulator.getField(myFields)!=null) return false;
            } else if(!field.equals(oldKeyAccumulator.getField(myFields))) return false;
        }
        return true;
    }

    @Override
    public boolean hasField(int myFields) {
        return !remainingFields.get(myFields);
    }

    @Override
    public ByteBuffer getField(int myFields) {
        return fields[myFields];
    }
}
