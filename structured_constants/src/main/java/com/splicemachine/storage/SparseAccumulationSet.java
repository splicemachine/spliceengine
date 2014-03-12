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
		public void reset() {
				super.reset();
				remainingFields = (BitSet)allFields.clone();
		}
}
