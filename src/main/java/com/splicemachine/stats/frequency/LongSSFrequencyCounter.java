package com.splicemachine.stats.frequency;


import com.splicemachine.hash.Hash32;

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
														 Hash32 hashFunction) {
				super(maxSize, initialCapacity, hashFunction);
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
    public void update(long item,long count) {
        LongElement staleElement = doPut(item,count);
        if(staleElement!=null){
            //adding a new element. If necessary, evict an old
            size++;
            staleElement.epsilon = evictAndSet(staleElement);
            staleElement.value = item;
            increment(staleElement,count);
            staleElement.stale = false;
            elementsSeen++;
        }
        if(size>=expansionLimit){
            //make sure we are big enough for the next one
            expandCapacity();
        }
    }

    /*****************************************************************************************************************/
    /*private helper methods*/
    private LongElement doPut(long item, long count){
        int hashCode = hash(item,hashFunction);
        int pos = hashCode & (capacity - 1);
        int probeLength=0;
        LongElement staleElement = null;
        LongElement toPlace = null;
        while(true){
            int hC = hashCodes[pos];
            if(hC==hashCode){
                LongElement e = (LongElement)hashtable[pos];
                if(e==null){
                    if(toPlace==null) toPlace= staleElement= (LongElement)newElement();

                    hashtable[pos] = toPlace;
                    break;
                }else if(e.stale){
                    staleElement = e;
                    break;
                }else if(e.matchingValue(item)){
                    elementsSeen++;
                    increment(e,count);
                    return null;
                }
            }else if(hC==0){
                //we have an empty slot, so place it directly
                hashCodes[pos] = hashCode;
                if(toPlace==null) toPlace = staleElement = (LongElement)newElement();
                hashtable[pos] = toPlace;
                break;
            }
            int pC = getProbeDistance(pos,hC);
            if(pC>probeLength){
                /*
                 * We've reached an element with a shorter probe length than us. In this case,
                 * swap the element, and adjust the probe length
                 */
                LongElement e = (LongElement)hashtable[pos];
                hashCodes[pos] = hashCode;
                if(toPlace==null) toPlace = staleElement = (LongElement)newElement();
                hashtable[pos] = toPlace;
                toPlace= e;
                probeLength = pC;
                hashCode = hC;
            }
            probeLength++;
            pos = (pos+1) & (capacity-1);
        }
        if(longestProbeLength<probeLength)
            longestProbeLength = probeLength;
        return staleElement;
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
