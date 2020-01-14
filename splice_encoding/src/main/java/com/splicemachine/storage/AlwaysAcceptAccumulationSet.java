/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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
						remainingFields.union(occupiedFields); //set back the occupied fields
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
