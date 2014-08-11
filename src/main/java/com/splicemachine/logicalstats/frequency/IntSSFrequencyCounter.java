package com.splicemachine.logicalstats.frequency;

import com.splicemachine.utils.hash.Hash32;

/**
 * @author Scott Fines
 * Date: 3/26/14
 */
class IntSSFrequencyCounter extends SSFrequencyCounter<Integer> implements IntFrequencyCounter {
		protected IntSSFrequencyCounter(int maxSize, int initialCapacity, Hash32[] hashFunctions) {
				super(maxSize, initialCapacity, hashFunctions);
		}

		@Override protected Element[] newHashArray(int size) { return new IntElement[size]; }
		@Override protected Element newElement() { return new IntElement(); }
		@Override protected int hash(Element element, Hash32 hashFunction) { return hashFunction.hash(((IntElement)element).value); }
		@Override protected int hash(Integer item, Hash32 hashFunction) { return hashFunction.hash(item); }

		@Override
		public void update(Integer item) {
				update(item,1l);
		}

		@Override
		public void update(Integer item, long count) {
				assert item!=null: "Null items not allowed!";
				update(item.intValue(), count);
		}

		@Override
		public void update(int item) {

		}

		@Override
		public void update(int item,long count) {
				IntElement staleElement = null;
				int visitedCount = 0;
				HASH_LOOP:for (Hash32 hashFunction : hashFunctions) {
						int pos = hashFunction.hash(item) & (capacity - 1);
						for(int i=0;i<3 && visitedCount<capacity;i++){
								int newPos = (pos+i) & (capacity-1);
								IntElement existing = (IntElement)hashtable[newPos];
								if (existing == null) {
										if(staleElement==null){
												existing = new IntElement();
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

		private class IntElement extends Element{
				private int value;
				@Override public void setValue(Integer value) { this.value = value;  }
				@Override public boolean matchingValue(Integer item) { return value == item; }
				@Override public Integer value() { return value; }

				@Override
				public boolean equals(Object o) {
						if (this == o) return true;
						if (!(o instanceof IntElement)) return false;
						IntElement that = (IntElement) o;
						return value == that.value;
				}

				@Override public int hashCode() { return value; }
		}
}
