package com.splicemachine.stats.frequency;


import com.splicemachine.hash.Hash32;

/**
 * @author Scott Fines
 * Date: 3/26/14
 */
class ShortSSFrequencyCounter extends SSFrequencyCounter<Short> implements ShortFrequencyCounter {
		public ShortSSFrequencyCounter(int maxSize,
																	 int initialCapacity,
																	 Hash32 hashFunction) {
				super(maxSize, initialCapacity, hashFunction);
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
        ShortElement staleElement = doPut(item,count);
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

    @Override
    public ShortFrequentElements heavyHitters(float support) {
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public ShortFrequentElements frequentElements(int k) {
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    /*****************************************************************************************************************/
    /*private helper methods*/
    private ShortElement doPut(short item, long count){
        int hashCode = hash(item,hashFunction);
        int pos = hashCode & (capacity - 1);
        int probeLength=0;
        ShortElement staleElement = null;
        ShortElement toPlace = null;
        while(true){
            int hC = hashCodes[pos];
            if(hC==hashCode){
                ShortElement e = (ShortElement)hashtable[pos];
                if(e==null){
                    if(toPlace==null) toPlace= staleElement= (ShortElement)newElement();

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
                if(toPlace==null) toPlace = staleElement = (ShortElement)newElement();
                hashtable[pos] = toPlace;
                break;
            }
            int pC = getProbeDistance(pos,hC);
            if(pC>probeLength){
                /*
                 * We've reached an element with a shorter probe length than us. In this case,
                 * swap the element, and adjust the probe length
                 */
                ShortElement e = (ShortElement)hashtable[pos];
                hashCodes[pos] = hashCode;
                if(toPlace==null) toPlace = staleElement = (ShortElement)newElement();
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

		private class ShortElement extends Element implements ShortFrequencyEstimate{
				short value;
				@Override public void setValue(Short value) { this.value = value; }
				@Override public boolean matchingValue(Short item) { return value == item; }
        @Override public short value() { return value; }
				@Override public Short getValue() { return value; }

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
