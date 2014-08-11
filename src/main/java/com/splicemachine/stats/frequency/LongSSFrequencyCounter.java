package com.splicemachine.stats.frequency;

import com.splicemachine.utils.hash.Hash32;

/**
 * Implementation of {@link SSFrequencyCounter} which is specific for Longs. This
 * avoids autoboxing in all situations.
 *
 * @author Scott Fines
 * Date: 3/24/14
 */
class LongSSFrequencyCounter extends SSFrequencyCounter<Long> implements LongFrequencyCounter {

		protected LongSSFrequencyCounter(int maxSize,
														 int initialCapacity,
														 Hash32[] hashFunctions) {
				super(maxSize, initialCapacity, hashFunctions);
		}

		@Override protected int hash(Long item, Hash32 hashFunction) { return hashFunction.hash(item); }
		@Override protected int hash(Element element, Hash32 hashFunction) { return hashFunction.hash(((LongElement)element).value); }
		@Override protected Element[] newHashArray(int size) { return new LongElement[size]; }
		@Override protected Element newElement() { return new LongElement(); }

		/**
		 * Whenever possible, avoid using this method, as it will autobox the item down to a primitive, then call
		 * {@link #update(long)}
		 */
		@Override
		public final void update(Long item) {
				update(item,1l);
		}

		@Override
		public void update(Long item, long count) {
				assert item!=null: "Null values are not allowed!";
				update(item.longValue(), count);
		}

		@Override public void update(long item) { update(item,1l); }

		@Override
		public final void update(long item,long count) {
				LongElement staleElement = null;
				int visitedCount = 0;
				HASH_LOOP:for (Hash32 hashFunction : hashFunctions) {
						int pos = hashFunction.hash(item) & (capacity - 1);
						for(int i=0;i<3 && visitedCount<capacity;i++){
								int newPos = (pos+i) & (capacity-1);
								LongElement existing = (LongElement)hashtable[newPos];
								if (existing == null) {
										if(staleElement==null){
												existing = new LongElement();
												hashtable[newPos] = existing;
												staleElement = existing;
										}
										break HASH_LOOP;
								}if (existing.stale) {
										if(staleElement==null)
												staleElement = existing;
								}else if (existing.value == item) {
										elementsSeen++;
										increment(existing,count);
										return;
								}
								visitedCount++;
						}
				}
				if(staleElement!=null){
						//adding a new element. If necessary, evict an old
						size++;
						staleElement.epsilon = evictAndSet(staleElement);
						staleElement.value = item;
						increment(staleElement,count);
						staleElement.stale = false;
						elementsSeen++;
				}else{
						expandCapacity();
						update(item);
				}
		}

		private class LongElement extends Element{
				long value;
				@Override public void setValue(Long value) { this.value = value; }
				@Override public boolean matchingValue(Long item) { return value == item; }
				@Override public Long value() { return value; }

				@Override
				public boolean equals(Object o) {
						if (this == o) return true;
						if (!(o instanceof LongElement)) return false;

						LongElement that = (LongElement) o;
						return value == that.value;
				}

				@Override public int hashCode() { return (int) (value ^ (value >>> 32)); }
		}
}
