package com.splicemachine.stats.frequency;

import com.splicemachine.hash.Hash32;

/**
 * @author Scott Fines
 *         Date: 3/26/14
 */
class FloatSSFrequencyCounter extends SSFrequencyCounter<Float> implements FloatFrequencyCounter {
		protected FloatSSFrequencyCounter(int maxSize, int initialCapacity, Hash32[] hashFunctions) {
				super(maxSize, initialCapacity, hashFunctions);
		}

		@Override protected Element newElement() { return new FloatElement(); }
		@Override protected Element[] newHashArray(int size) { return new FloatElement[size]; }
		@Override protected int hash(Element element, Hash32 hashFunction) { return hash(((FloatElement) element).value, hashFunction); }
		@Override protected int hash(Float item, Hash32 hashFunction) { return hash((float)item,hashFunction); }

		@Override public void update(Float item) { update(item,1l); }

		@Override
		public void update(Float item, long count) {
				assert item!=null: "No nulls allowed!";
				update(item.floatValue(),count);
		}

		@Override
		public void update(float item, long count) {
				FloatElement staleElement = null;
				int visitedCount = 0;
				HASH_LOOP:for (Hash32 hashFunction : hashFunctions) {
						if(visitedCount>=capacity) break;
						int pos = hash(item,hashFunction) & (capacity - 1);
						for(int i=0;i<3 && visitedCount<capacity;i++){
								int newPos = (pos+i) & (capacity-1);
								FloatElement existing = (FloatElement)hashtable[newPos];
								if (existing == null) {
										if(staleElement==null){
												existing = new FloatElement();
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

		@Override
		public void update(float item) {
			update(item,1l);
		}



		private int hash(float item, Hash32 hashFunction){
				int rawBits = Float.floatToRawIntBits(item);
				return hashFunction.hash(rawBits);
		}

		private class FloatElement extends Element{
				private float value;
				@Override public void setValue(Float value) { this.value = value; }
				@Override public boolean matchingValue(Float item) { return item == value; }
				@Override public Float value() { return value; }

				@Override
				public boolean equals(Object o) {
						if (this == o) return true;
						if (!(o instanceof FloatElement)) return false;
						FloatElement that = (FloatElement) o;
						return Float.compare(that.value, value) == 0;
				}

				@Override public int hashCode() { return (value != +0.0f ? Float.floatToIntBits(value) : 0); }
		}
}
