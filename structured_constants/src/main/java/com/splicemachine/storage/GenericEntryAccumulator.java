package com.splicemachine.storage;

import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;
import java.nio.ByteBuffer;
import java.util.Arrays;
import com.carrotsearch.hppc.BitSet;

/**
 * @author Scott Fines
 * Created on: 10/2/13
 */
abstract class GenericEntryAccumulator implements EntryAccumulator{

    protected BitSet occupiedFields;

    protected ByteBuffer[] fields;
    private final boolean returnIndex;

    private BitSet scalarFields;
    private BitSet floatFields;
    private BitSet doubleFields;
    private EntryPredicateFilter predicateFilter;


    protected GenericEntryAccumulator(boolean returnIndex) {
        this(null,returnIndex);
    }

    protected GenericEntryAccumulator(int size,boolean returnIndex) {
        this(null,size,returnIndex);
    }

    protected GenericEntryAccumulator(EntryPredicateFilter filter,boolean returnIndex) {
        this.returnIndex = returnIndex;
        this.predicateFilter =filter;
        
        if(returnIndex){
            scalarFields = new BitSet();
            floatFields = new BitSet();
            doubleFields = new BitSet();
        }
        occupiedFields = new BitSet();
    }

    protected GenericEntryAccumulator(EntryPredicateFilter filter,int size,boolean returnIndex) {
        this.returnIndex = returnIndex;

        this.predicateFilter = filter;
        if(returnIndex){
            scalarFields = new BitSet(size);
            floatFields = new BitSet(size);
            doubleFields = new BitSet(size);
        }
        occupiedFields = new BitSet(size);
        fields = new ByteBuffer[size];
    }

    @Override
    public void add(int position, ByteBuffer buffer) {
        if(occupiedFields.get(position)) return; //already populated that field

        fields[position] = buffer;
        if(buffer!=null)
            buffer.mark();
        occupiedFields.set(position);
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
    public byte[] finish() {
        if(predicateFilter!=null){
            predicateFilter.reset();
            BitSet checkColumns = predicateFilter.getCheckedColumns();
            if(fields!=null){
                for(int i=checkColumns.nextSetBit(0);i>=0;i=checkColumns.nextSetBit(i+1)){

                    if(i>=fields.length||fields[i]==null){
                        if(!predicateFilter.checkPredicates(null,i)) return null;
                    }else{
                        ByteBuffer buffer = fields[i];
                        buffer.reset();
                        if(!predicateFilter.checkPredicates(buffer,i)) return null;
                    }
                }
            }else{
                for(int i=0;i<checkColumns.length();i++){
                        if(!predicateFilter.checkPredicates(null,i)) return null;
                }
            }

            predicateFilter.rowReturned();
        }

        byte[] dataBytes = getDataBytes();
        if(returnIndex){
            BitIndex index = BitIndexing.uncompressedBitMap(occupiedFields,scalarFields,floatFields,doubleFields);
            byte[] indexBytes = index.encode();

            byte[] finalBytes = new byte[indexBytes.length+dataBytes.length+1];
            System.arraycopy(indexBytes,0,finalBytes,0,indexBytes.length);
            System.arraycopy(dataBytes,0,finalBytes,indexBytes.length+1,dataBytes.length);
            return finalBytes;
        }
        return dataBytes;
    }

    @Override
    public void reset() {
        occupiedFields.clear();
        if(fields!=null)
            Arrays.fill(fields,null);

        if(predicateFilter!=null)
            predicateFilter.reset();
    }

    @Override
    public boolean hasField(int myFields) {
        return occupiedFields.get(myFields);
    }

    @Override
    public ByteBuffer getField(int myFields) {
        return fields[myFields];
    }

    protected byte[] getDataBytes() {
        int size=0;
        boolean isFirst=true;
        for(int n = occupiedFields.nextSetBit(0);n>=0;n=occupiedFields.nextSetBit(n+1)){
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
        for(int n=occupiedFields.nextSetBit(0);n>=0;n=occupiedFields.nextSetBit(n+1)){
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
    public boolean fieldsMatch(EntryAccumulator oldKeyAccumulator) {
        for(int myFields=occupiedFields.nextSetBit(0);myFields>=0;myFields=occupiedFields.nextSetBit(myFields+1)){
            if(!oldKeyAccumulator.hasField(myFields)) return false;

            ByteBuffer myField = fields[myFields];
            ByteBuffer theirField = oldKeyAccumulator.getField(myFields);
            if(myField==null){
                if(theirField!=null) return false;
            }else if(!myField.equals(theirField)) return false;
        }
        return true;
    }

}
