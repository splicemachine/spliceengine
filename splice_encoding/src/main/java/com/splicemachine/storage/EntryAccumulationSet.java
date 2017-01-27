/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
