package com.splicemachine.storage;

import com.splicemachine.storage.index.BitIndex;

import java.util.BitSet;

/**
 * @author Scott Fines
 *         Date: 3/11/14
 */
public class AlwaysAcceptAccumulationSet extends EntryAccumulationSet {
		private boolean completed = false;

		private BitSet remainingFields;

		@Override
		public BitSet remainingFields() {
				if(remainingFields==null){
				/*
         * We always want an entry, because we want to ensure that we run until the entire row is
         * populated, which means running until the end of all versions.
         */
						remainingFields = new BitSet();
						remainingFields.set(0,1024);
				}

				remainingFields.andNot(occupiedFields);
				return remainingFields;
		}

		@Override
		public void reset() {
				if(remainingFields!=null)
						remainingFields =(BitSet)occupiedFields.clone(); // JL TODO Remove clone op
				super.reset();
		}

		@Override public boolean isFinished() { return completed; }

		@Override
    public boolean isInteresting(BitIndex potentialIndex) {
        for(int i=potentialIndex.nextSetBit(0);i>=0;i=potentialIndex.nextSetBit(i+1)){
            if(!occupiedFields.get(i)) return true;
        }
        return false;
    }

		public void complete(){ this.completed = true; }

		@Override public boolean isInteresting(int position) { return occupiedFields.get(position); }
}
