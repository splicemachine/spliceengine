package com.splicemachine.storage;

import com.splicemachine.constants.bytes.BytesUtil;

import java.nio.ByteBuffer;
import java.util.BitSet;

/**
 * @author Scott Fines
 * Created on: 7/9/13
 */
class AlwaysAcceptEntryAccumulator implements EntryAccumulator {
    private BitSet occupiedFields;

    private ByteBuffer[] fields;

    AlwaysAcceptEntryAccumulator() {
        this.occupiedFields = new BitSet();
    }

    @Override
    public void add(int position, ByteBuffer buffer) {
        if(occupiedFields.get(position)) return; //this position is already set, don't set it again

        growFields(position); //make sure we're big enough
        fields[position] = buffer;
        occupiedFields.set(position);
    }

    private void growFields(int position) {
        /*
         * Make sure that the fields array is large enough to hold elements up to position.
         */
        if(fields==null){
            fields = new ByteBuffer[position+1];
        }else if(fields.length<=position){
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
        for(int i=0;i<fields.length;i++){
            if(fields[i]==null)
                bitSet.set(i);
        }
        /*
         * We always want an entry, because we want to ensure that we run until the entire row is
         * populated, which means running until the end of all versions.
         */
        if(bitSet.cardinality()==0){
            bitSet.set(0);
        }
        return bitSet;
    }

    @Override
    public byte[] finish() {
        return BytesUtil.concatenate(fields);
    }

    @Override
    public void reset() {
        occupiedFields.clear();
    }
}
