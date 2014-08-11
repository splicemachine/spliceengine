package com.splicemachine.stats.frequency;

import com.splicemachine.utils.hash.Hash32;

/**
 * @author Scott Fines
 * Date: 3/26/14
 */
class ShortSSFrequencyCounter extends SSFrequencyCounter<Short> implements ShortFrequencyCounter {
		public ShortSSFrequencyCounter(int maxSize,
																	 int initialCapacity,
																	 Hash32[] hashFunctions) {
				super(maxSize, initialCapacity, hashFunctions);
		}

		@Override protected Element newElement() { return new ShortElement(); }
		@Override protected Element[] newHashArray(int size) { return new ShortElement[size]; }
		@Override protected int hash(Short item, Hash32 hashFunction) { return hashFunction.hash(item); }
		@Override protected int hash(Element element, Hash32 hashFunction) { return hashFunction.hash(((ShortElement)element).value); }

		@Override public void update(Short item) { update(item,1l); }

		@Override
		public void update(Short item, long count) {
				assert item!=null: "Null elements not allowed!";
				update(item.shortValue(),count);
		}

		@Override
		public void update(short item) {
			update(item,1l);
		}

		@Override
		public void update(short item,long count) {
				ShortElement staleElement = null;
				int visitedCount = 0;
				HASH_LOOP:for (Hash32 hashFunction : hashFunctions) {
						int pos = hashFunction.hash(item) & (capacity - 1);
						for(int i=0;i<3 && visitedCount<capacity;i++){
								int newPos = (pos+i) & (capacity-1);
								ShortElement existing = (ShortElement)hashtable[newPos];
								if (existing == null) {
										if(staleElement==null){
												existing = new ShortElement();
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

		private class ShortElement extends Element{
				short value;
				@Override public void setValue(Short value) { this.value = value; }
				@Override public boolean matchingValue(Short item) { return value == item; }
				@Override public Short value() { return value; }

				@Override
				public boolean equals(Object o) {
						if (this == o) return true;
						if (!(o instanceof ShortElement)) return false;
						ShortElement that = (ShortElement) o;
						return value == that.value;
				}

				@Override public int hashCode() { return (int) value; }
		}
}
