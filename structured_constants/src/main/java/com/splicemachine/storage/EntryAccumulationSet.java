package com.splicemachine.storage;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.storage.index.BitIndexing;


/**
 * @author Scott Fines
 *         Date: 3/11/14
 */
abstract class EntryAccumulationSet {
		BitSet occupiedFields;
		private BitSet scalarFields;
		private BitSet floatFields;
		private BitSet doubleFields;

		public EntryAccumulationSet() {
				this.occupiedFields = new BitSet();
				this.scalarFields = new BitSet();
				this.floatFields = new BitSet();
				this.doubleFields = new BitSet();
		}

		public void addScalar(int position){
				occupy(position);
				scalarFields.set(position);
		}


		public void addFloat(int position){
				occupy(position);
				floatFields.set(position);
		}

		public void addDouble(int position){
				occupy(position);
				doubleFields.set(position);
		}

		public void addUntyped(int position){
				occupy(position);
		}

		public void reset(){
				occupiedFields.clear();

		}

		public byte[] encode(){
				BitIndex index = BitIndexing.uncompressedBitMap(occupiedFields,scalarFields,floatFields,doubleFields);
				return index.encode();
		}

		protected void occupy(int position) {
				occupiedFields.set(position);
		}

		public boolean get(int position) {
				return occupiedFields.get(position);
		}

		public abstract BitSet remainingFields();

		public abstract boolean isFinished();

		public abstract boolean isInteresting(BitIndex potentialIndex);

		public abstract void complete();
}
