package com.splicemachine.storage;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.storage.index.BitIndex;

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
//				for(int i=occupiedFields.nextSetBit(0);i>=0;i=occupiedFields.nextSetBit(i+1))
//						bitSet.clear(i);
		}

		@Override
		public void reset() {
				if(remainingFields!=null)
						remainingFields.union(occupiedFields); //set back the occupied fields
				super.reset();
		}

		@Override public boolean isFinished() { return completed; }

		@Override public boolean isInteresting(BitIndex potentialIndex) { return true; }

		public void complete(){ this.completed = true; }

		@Override public boolean isInteresting(int position) { return occupiedFields.get(position); }
}
