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
