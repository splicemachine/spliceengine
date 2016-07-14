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

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 3/11/14
 */
public class SparseAccumulationSet extends EntryAccumulationSet {
		private BitSet remainingFields;
		private BitSet allFields;

		private Map<BitSet,byte[]> encodedIndexCache = new LinkedHashMap<BitSet, byte[]>(1){
				@Override
				protected boolean removeEldestEntry(Map.Entry<BitSet, byte[]> eldest) {
						return encodedIndexCache.size()>100; //only keep 100 elements in the cache
				}
		};
		public SparseAccumulationSet(BitSet allFields) {
				this.allFields = allFields;
				this.remainingFields = (BitSet)allFields.clone();
		}

		@Override
		protected void occupy(int position) {
				super.occupy(position);
				remainingFields.clear(position);
		}

		@Override
		public byte[] encode() {
				byte[] preEncoded = encodedIndexCache.get(remainingFields);
				if(preEncoded==null){
						preEncoded = super.encode();
						encodedIndexCache.put(remainingFields,preEncoded);
				}
				return preEncoded;
		}

		@Override public BitSet remainingFields() { return remainingFields; }

		@Override public boolean isFinished() { return remainingFields.cardinality()<=0; }

		@Override
		public boolean isInteresting(BitIndex potentialIndex) {
				return potentialIndex.intersects(remainingFields);
		}

		//no-op
		@Override public void complete() {  }

		@Override
		public boolean isInteresting(int position) {
				return allFields.get(position);
		}

		@Override
		public void reset() {
				super.reset();
				remainingFields.union(allFields);
		}
}
