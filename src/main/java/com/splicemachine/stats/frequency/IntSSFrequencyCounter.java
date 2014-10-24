package com.splicemachine.stats.frequency;


import com.splicemachine.hash.Hash32;

/**
 * @author Scott Fines
 * Date: 3/26/14
 */
class IntSSFrequencyCounter extends SSFrequencyCounter<Integer> implements IntFrequencyCounter {
		protected IntSSFrequencyCounter(int maxSize, int initialCapacity, Hash32 hashFunction) {
				super(maxSize, initialCapacity, hashFunction);
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
        update(item,1l);
		}

		@Override
		public void update(int item,long count) {
				IntElement staleElement = doPut(item,count);
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

    private IntElement doPut(int item, long count){
        int hashCode = hash(item,hashFunction);
        int pos = hashCode & (capacity - 1);
        int probeLength=0;
        IntElement staleElement = null;
        IntElement toPlace = null;
        while(true){
            int hC = hashCodes[pos];
            if(hC==hashCode){
                IntElement e = (IntElement)hashtable[pos];
                if(e==null){
                    if(toPlace==null) toPlace= staleElement= (IntElement)newElement();

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
                if(toPlace==null) toPlace = staleElement = (IntElement)newElement();
                hashtable[pos] = toPlace;
                break;
            }
            int pC = getProbeDistance(pos,hC);
            if(pC>probeLength){
                /*
                 * We've reached an element with a shorter probe length than us. In this case,
                 * swap the element, and adjust the probe length
                 */
                IntElement e = (IntElement)hashtable[pos];
                hashCodes[pos] = hashCode;
                if(toPlace==null) toPlace = staleElement = (IntElement)newElement();
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
