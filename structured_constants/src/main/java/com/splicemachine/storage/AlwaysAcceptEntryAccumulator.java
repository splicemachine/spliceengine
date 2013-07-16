package com.splicemachine.storage;

import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * @author Scott Fines
 * Created on: 7/9/13
 */
class AlwaysAcceptEntryAccumulator implements EntryAccumulator {
    private BitSet occupiedFields;

    private ByteBuffer[] fields;

    private boolean completed;

    private boolean returnIndex;
    private BitSet scalarFields = new BitSet();
    private BitSet floatFields = new BitSet();
    private BitSet doubleFields = new BitSet();

    private EntryPredicateFilter predicateFilter;

    AlwaysAcceptEntryAccumulator(EntryPredicateFilter predicateFilter){
        this(predicateFilter,false);
    }

    AlwaysAcceptEntryAccumulator(EntryPredicateFilter predicateFilter,boolean returnIndex) {
        this.occupiedFields = new BitSet();
        this.completed = false;
        this.returnIndex = returnIndex;
        this.predicateFilter = predicateFilter;
    }

    @Override
    public void add(int position, ByteBuffer buffer) {
        if(occupiedFields.get(position)) return; //this position is already set, don't set it again

        growFields(position); //make sure we're big enough
        fields[position] = buffer;
        buffer.mark();
        occupiedFields.set(position);
    }

    @Override
    public void addScalar(int position, ByteBuffer buffer) {
        add(position,buffer);
        scalarFields.set(position);
    }

    @Override
    public void addFloat(int position, ByteBuffer buffer) {
        add(position,buffer);
        floatFields.set(position);
    }

    @Override
    public void addDouble(int position, ByteBuffer buffer) {
        add(position,buffer);
        doubleFields.set(position);
    }

    public void complete(){
        this.completed = true;
    }

    private void growFields(int position) {
        /*
         * Make sure that the fields array is large enough to hold elements up to position.
         */
        if(fields==null){
            fields = new ByteBuffer[position+1];
        }else if(fields.length<=position && !completed){ //if completed, we know how many to return
            //grow the fields list to be big enough to hold the position

            /*
             * In Normal circumstances, we would grow by some efficient factor
             * like 3/2 or something, so that we don't have to copy out entries over and over again.
             *
             * However, in this case, we can't grow past what we need, because that would place additional
             * null entries at the end of our return array. Instead, we must only be as large as needed
             * to hold position.
             *
             * This isn't so bad, though--once the first row has been resolved, we should never have
             * to grow again, so we'll pay a penalty on the first row only.
             */
            int newSize = position+1;
            ByteBuffer[] oldFields = fields;
            fields = new ByteBuffer[newSize];
            System.arraycopy(oldFields,0,fields,0,oldFields.length);
        }
    }

    @Override
    public BitSet getRemainingFields() {
        BitSet bitSet = new BitSet();
        if(fields!=null){
            for(int i=0;i<fields.length;i++){
                if(fields[i]==null)
                    bitSet.set(i);
            }
        }
        /*
         * We always want an entry, because we want to ensure that we run until the entire row is
         * populated, which means running until the end of all versions.
         */
        if(!completed){
            if(fields!=null)
                bitSet.set(fields.length,1024);
            else
                bitSet.set(0,1024);
        }
        return bitSet;
    }

    @Override
    public byte[] finish() {
        //check final predicates to make sure that we don't miss any
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
        byte[] dataBytes = getDataBytes();
        if(returnIndex){
            BitSet setFields = new BitSet(fields.length);
            for(int i=0;i<fields.length;i++){
                if(fields[i]!=null)
                    setFields.set(i);
            }
            BitIndex index = BitIndexing.getBestIndex(setFields,scalarFields,floatFields,doubleFields);
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
        for(int i=0;i<fields.length;i++){
            if(isFirst)isFirst=false;
            else
                size++;
            ByteBuffer buffer = fields[i];
            if(buffer!=null){
                buffer.reset();
                size+=buffer.remaining();
            }
        }

        byte[] bytes = new byte[size];
        int offset=0;
        isFirst=true;
        for(int i=0;i<fields.length;i++){
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
    public void reset() {
        if(occupiedFields!=null)
            occupiedFields.clear();
        if(fields!=null){
            for(int i=0;i<fields.length;i++){
                fields[i] = null;
            }
        }
    }

    @Override
    public boolean fieldsMatch(EntryAccumulator oldKeyAccumulator) {
        for(int myFields=occupiedFields.nextSetBit(0);myFields>=0;myFields=occupiedFields.nextSetBit(myFields+1)){
            if(!oldKeyAccumulator.hasField(myFields)) return false;
        }
        return true;
    }

    @Override
    public boolean hasField(int myFields) {
        return occupiedFields.get(myFields);
    }

    @Override
    public ByteBuffer getField(int myFields) {
        return fields[myFields];
    }
}
