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

/**
 * @author Scott Fines
 *         Date: 3/11/14
 */
public abstract class BaseEntryAccumulator<T extends EntryAccumulator<T>> implements EntryAccumulator<T>{

		protected EntryAccumulationSet accumulationSet;

		protected final boolean returnIndex;

		protected final EntryPredicateFilter predicateFilter;
		protected long finishCount;

		protected BaseEntryAccumulator( EntryPredicateFilter predicateFilter,boolean returnIndex,BitSet fieldsToCollect) {
				this.returnIndex = returnIndex;
				this.predicateFilter = predicateFilter;

				if(fieldsToCollect!=null && !fieldsToCollect.isEmpty()){
						this.accumulationSet = new SparseAccumulationSet(fieldsToCollect);
				}else
						this.accumulationSet = new AlwaysAcceptAccumulationSet();
		}

		@Override
		public void add(int position, byte[] data, int offset, int length) {
				if(accumulationSet.get(position))
						return;
				occupy(position, data, offset, length);
				accumulationSet.addUntyped(position);
		}


		@Override
		public void addScalar(int position, byte[] data, int offset, int length) {
				if(accumulationSet.get(position))
						return;
				occupyScalar(position,data,offset,length);
				accumulationSet.addScalar(position);
		}

		@Override
		public void addFloat(int position, byte[] data, int offset, int length) {
				if(accumulationSet.get(position))
						return;
				occupyFloat(position,data,offset,length);
				accumulationSet.addFloat(position);
		}

		@Override
		public void addDouble(int position, byte[] data, int offset, int length) {
				if(accumulationSet.get(position))
						return;
				occupyDouble(position,data,offset,length);
				accumulationSet.addDouble(position);
		}

		protected abstract void occupy(int position, byte[] data, int offset, int length);
		protected abstract void occupyDouble(int position, byte[] data, int offset, int length);
		protected abstract void occupyFloat(int position, byte[] data, int offset, int length);
		protected abstract void occupyScalar(int position, byte[] data, int offset, int length);

		
		
		@Override public BitSet getRemainingFields() { return accumulationSet.remainingFields(); }
		@Override public boolean isFinished() { return accumulationSet.isFinished(); }
		@Override
		public void reset() {
				accumulationSet.reset();
				if(predicateFilter!=null)
						predicateFilter.reset();
		}

		@Override
		public boolean fieldsMatch(T oldKeyAccumulator) {
				BitSet occupiedFields = accumulationSet.occupiedFields;
				for(int myFields=occupiedFields.nextSetBit(0);myFields>=0;myFields=occupiedFields.nextSetBit(myFields+1)){
						if(!oldKeyAccumulator.hasField(myFields)) return false;

						if(!matchField(myFields,oldKeyAccumulator)) return false;
				}
				return true;
		}

		protected abstract boolean matchField(int myFields,T otherAccumulator);

		@Override public boolean hasField(int myFields) { return accumulationSet.get(myFields); }
		@Override public long getFinishCount() { return finishCount; }
		@Override public void markOccupiedScalar(int position) { accumulationSet.addScalar(position); }
		@Override public void markOccupiedFloat(int position) { accumulationSet.addFloat(position); }
		@Override public void markOccupiedDouble(int position) { accumulationSet.addDouble(position); }
		@Override public void markOccupiedUntyped(int position) { accumulationSet.addUntyped(position); }

		@Override
		public boolean isInteresting(BitIndex potentialIndex) {
				return accumulationSet.isInteresting(potentialIndex);
		}

		@Override public void complete() { accumulationSet.complete(); }
}
