package com.splicemachine.stats.frequency;

import com.splicemachine.hash.Hash32;

/**
 * @author Scott Fines
 *         Date: 3/26/14
 */
class DoubleSSFrequencyCounter extends SSFrequencyCounter<Double> implements DoubleFrequencyCounter {
		protected DoubleSSFrequencyCounter(int maxSize, int initialCapacity, Hash32 hashFunction) {
				super(maxSize, initialCapacity, hashFunction);
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
    public void update(double item,long count) {
        DoubleElement staleElement = doPut(item,count);
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
		public void update(double item) {
			update(item,1l);
		}

    /*****************************************************************************************************************/
    /*private helper methods*/
    private DoubleElement doPut(double item, long count){
        int hashCode = hash(item,hashFunction);
        int pos = hashCode & (capacity - 1);
        int probeLength=0;
        DoubleElement staleElement = null;
        DoubleElement toPlace = null;
        while(true){
            int hC = hashCodes[pos];
            if(hC==hashCode){
                DoubleElement e = (DoubleElement)hashtable[pos];
                if(e==null){
                    if(toPlace==null) toPlace= staleElement= (DoubleElement)newElement();

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
                if(toPlace==null) toPlace = staleElement = (DoubleElement)newElement();
                hashtable[pos] = toPlace;
                break;
            }
            int pC = getProbeDistance(pos,hC);
            if(pC>probeLength){
                /*
                 * We've reached an element with a shorter probe length than us. In this case,
                 * swap the element, and adjust the probe length
                 */
                DoubleElement e = (DoubleElement)hashtable[pos];
                hashCodes[pos] = hashCode;
                if(toPlace==null) toPlace = staleElement = (DoubleElement)newElement();
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

		private int hash(double element, Hash32 hashFunction){
				long val = Double.doubleToLongBits(element);
				return hashFunction.hash(val);
		}

    @Override
    public DoubleFrequentElements heavyHitters(float support) {
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public DoubleFrequentElements frequentElements(int k) {
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    private class DoubleElement extends Element{
				private double value;
				@Override public void setValue(Double value) { this.value = value; }
				@Override public boolean matchingValue(Double item) { return item == value; }
				@Override public Double getValue() { return value; }

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
