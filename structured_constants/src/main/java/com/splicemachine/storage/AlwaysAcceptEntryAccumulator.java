package com.splicemachine.storage;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.ByteSlice;


/**
 * @author Scott Fines
 * Created on: 7/9/13
 */
class AlwaysAcceptEntryAccumulator extends GenericEntryAccumulator {
    private boolean completed;


    AlwaysAcceptEntryAccumulator(EntryPredicateFilter predicateFilter){
        this(predicateFilter,false);
    }

    AlwaysAcceptEntryAccumulator(EntryPredicateFilter predicateFilter,boolean returnIndex) {
        super(predicateFilter,returnIndex);
        this.completed = false;
    }

		@Override
		public void add(int position, byte[] data, int offset, int length) {
				growFields(position);
				super.add(position, data, offset, length);
		}

    public void complete(){
        this.completed = true;
    }

    private void growFields(int position) {
        /*
         * Make sure that the fields array is large enough to hold elements up to position.
         */
        if(fields==null){
            fields = new ByteSlice[position+1];
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
            ByteSlice[] oldFields = fields;
            fields = new ByteSlice[newSize];
            System.arraycopy(oldFields,0,fields,0,oldFields.length);
        }
    }

    @Override
    public BitSet getRemainingFields() {
        /*
         * We always want an entry, because we want to ensure that we run until the entire row is
         * populated, which means running until the end of all versions.
         */
        BitSet bitSet = new BitSet();
				bitSet.set(0,1024);
				for(int i=occupiedFields.nextSetBit(0);i>=0;i=occupiedFields.nextSetBit(i+1))
						bitSet.clear(i);

//        if(fields!=null){
//            for(int i=0;i<fields.length;i++){
//                if(fields[i]==null||fields[i].length()<=0)
//                    bitSet.set(i);
//            }
//        }
//        if(!completed){
//            if(fields!=null)
//                bitSet.set(fields.length,1024);
//            else
//                bitSet.set(0,1024);
//        }
        return bitSet;
    }

		@Override
		public boolean isFinished() {
				return completed;
		}


		@Override
    public boolean hasField(int myFields) {
        return occupiedFields.get(myFields);
    }

		@Override public boolean isInteresting(BitIndex potentialIndex) { return true; }

}
