/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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

		public abstract boolean isInteresting(int position);
}
