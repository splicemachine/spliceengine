package com.splicemachine.logicalstats.frequency;

import com.splicemachine.utils.hash.Hash32;

/**
 * @author Scott Fines
 *         Date: 3/26/14
 */
class DoubleSSFrequencyCounter extends SSFrequencyCounter<Double> implements DoubleFrequencyCounter {
		protected DoubleSSFrequencyCounter(int maxSize, int initialCapacity, Hash32[] hashFunctions) {
				super(maxSize, initialCapacity, hashFunctions);
		}

		@Override protected Element[] newHashArray(int size) { return new DoubleElement[size]; }
		@Override protected Element newElement() { return new DoubleElement(); }
		@Override protected int hash(Element element, Hash32 hashFunction) { return hash(((DoubleElement)element).value,hashFunction); }
		@Override protected int hash(Double item, Hash32 hashFunction) { return hash((double)item,hashFunction); }

		@Override public void update(Double item) { update(item,1l); }

		@Override
		public void update(Double item, long count) {
				assert item!=null: "Null values are not allowed!";
				update(item.doubleValue());
		}

		@Override
		public void update(double item, long count) {
				DoubleElement staleElement = null;
				int visitedCount = 0;
				HASH_LOOP:for (Hash32 hashFunction : hashFunctions) {
						if(visitedCount>=capacity) break;
						int pos = hash(item,hashFunction) & (capacity - 1);
						for(int i=0;i<3 && visitedCount<capacity;i++){
								int newPos = (pos+i) & (capacity-1);
								DoubleElement existing = (DoubleElement)hashtable[newPos];
								if (existing == null) {
										if(staleElement==null){
												existing = new DoubleElement();
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
		public void update(double item) {
			update(item,1l);
		}

		private int hash(double element, Hash32 hashFunction){
				long val = Double.doubleToLongBits(element);
				return hashFunction.hash(val);
		}

		private class DoubleElement extends Element{
				private double value;
				@Override public void setValue(Double value) { this.value = value; }
				@Override public boolean matchingValue(Double item) { return item == value; }
				@Override public Double value() { return value; }

				@Override
				public boolean equals(Object o) {
						if (this == o) return true;
						if (!(o instanceof DoubleElement)) return false;
						DoubleElement that = (DoubleElement) o;
						return Double.compare(that.value, value) == 0;
				}

				@Override
				public int hashCode() {
						long temp = Double.doubleToLongBits(value);
						return (int) (temp ^ (temp >>> 32));
				}
		}
}
