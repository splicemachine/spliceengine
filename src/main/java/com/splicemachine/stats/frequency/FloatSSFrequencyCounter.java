package com.splicemachine.stats.frequency;

import com.splicemachine.hash.Hash32;

/**
 * @author Scott Fines
 *         Date: 3/26/14
 */
class FloatSSFrequencyCounter extends SSFrequencyCounter<Float> implements FloatFrequencyCounter {
		protected FloatSSFrequencyCounter(int maxSize, int initialCapacity, Hash32 hashFunction) {
				super(maxSize, initialCapacity, hashFunction);
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
				FloatElement staleElement = doPut(item,count);
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
						expandCapacity();
				}
		}

		@Override
		public void update(float item) {
			update(item,1l);
		}

    /**************************************************************************************************************/
    /*private helper methods*/
    private FloatElement doPut(float item, long count){
        int hashCode = hash(item,hashFunction);
        int pos = hashCode & (capacity - 1);
        int probeLength=0;
        FloatElement staleElement = null;
        FloatElement toPlace = null;
        while(true){
            int hC = hashCodes[pos];
            if(hC==hashCode){
                FloatElement e = (FloatElement)hashtable[pos];
                if(e==null){
                    if(toPlace==null) toPlace= staleElement= (FloatElement)newElement();

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
                if(toPlace==null) toPlace = staleElement = (FloatElement)newElement();
                hashtable[pos] = toPlace;
                break;
            }
            int pC = getProbeDistance(pos,hC);
            if(pC>probeLength){
                /*
                 * We've reached an element with a shorter probe length than us. In this case,
                 * swap the element, and adjust the probe length
                 */
                FloatElement e = (FloatElement)hashtable[pos];
                hashCodes[pos] = hashCode;
                if(toPlace==null) toPlace = staleElement = (FloatElement)newElement();
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
