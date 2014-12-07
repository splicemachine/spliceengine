package com.splicemachine.stats.frequency;


import com.splicemachine.hash.Hash32;
import com.splicemachine.primitives.ByteComparator;

import java.nio.ByteBuffer;

/**
 * @author Scott Fines
 *         Date: 3/26/14
 */
class BytesSSFrequencyCounter extends SSFrequencyCounter<byte[]> implements BytesFrequencyCounter {
    private final ByteComparator byteComparator;

		protected BytesSSFrequencyCounter(ByteComparator byteComparator,
                                      int maxSize, int initialCapacity, Hash32 hashFunction) {
				super(maxSize, initialCapacity, hashFunction);
        this.byteComparator = byteComparator;
		}

		@Override
		public BytesFrequentElements heavyHitters(float support) {
				throw new UnsupportedOperationException("IMPLEMENT");
		}

		@Override
		public BytesFrequentElements frequentElements(int k) {
				throw new UnsupportedOperationException("IMPLEMENT");
		}

		@Override
		protected int hash(byte[] item, Hash32 hashFunction) {
				return hashFunction.hash(item, 0, item.length);
		}

		@Override protected Element[] newHashArray(int size) { return new BytesElement[size]; }

		@Override protected Element newElement() { return new BytesElement(); }

		@Override
		public void update(byte[] item) {
				assert item!=null: "Null items are not allowed!";
				update(item,0,item.length);
		}

		@Override
		public void update(byte[] bytes, int offset, int length, long count) {
				BytesElement staleElement = doPut(bytes,offset,length,count);
				if(staleElement!=null){
						//adding a new element. If necessary, evict an old
						size++;
						staleElement.epsilon = evictAndSet(staleElement);
						staleElement.set(bytes,offset,length);
						increment(staleElement,count);
						staleElement.stale = false;
						elementsSeen++;
				}

        if(size>=expansionLimit)
						expandCapacity();
		}

		@Override
		public void update(ByteBuffer bytes, long count) {
				assert bytes!=null: "Cannot accept null values!";
				byte[] data = new byte[bytes.remaining()];
				bytes.get(data);
				update(data,0,data.length,count);
		}

		@Override
		public void update(byte[] item, long count) {
				update(item,0,item.length,count);
		}

		@Override
		public void update(byte[] bytes, int offset, int length) {
				update(bytes,offset,length,1l);
		}

		@Override
		public void update(ByteBuffer bytes) {
				update(bytes,1l);
		}

    protected BytesElement doPut(byte[] bytes, int offset, int length, long count){
        int hashCode = hashFunction.hash(bytes,offset,length);
        if(hashCode==0)
            hashCode = 1;
        int pos = hashCode & (capacity - 1);
        int probeLength=0;
        BytesElement staleElement = null;
        BytesElement toPlace = null;
        while(true){
            int hC = hashCodes[pos];
            if(hC==hashCode){
                BytesElement e = (BytesElement)hashtable[pos];
                if(e==null){
                    if(toPlace==null) toPlace= staleElement= (BytesElement)newElement();

                    hashtable[pos] = toPlace;
                    break;
                }else if(e.stale){
                    staleElement = e;
                    break;
                }else if(e.matchingValue(bytes,offset,length)){
                    elementsSeen++;
                    increment(e,count);
                    return null;
                }
            }else if(hC==0){
                //we have an empty slot, so place it directly
                hashCodes[pos] = hashCode;
                if(toPlace==null) toPlace = staleElement = (BytesElement)newElement();
                hashtable[pos] = toPlace;
                break;
            }
            int pC = getProbeDistance(pos,hC);
            if(pC>probeLength){
                /*
                 * We've reached an element with a shorter probe length than us. In this case,
                 * swap the element, and adjust the probe length
                 */
                BytesElement e = (BytesElement)hashtable[pos];
                hashCodes[pos] = hashCode;
                if(toPlace==null) toPlace = staleElement = (BytesElement)newElement();
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

		private class BytesElement extends Element{
				private byte[] value;
				private int offset;
				private int length;

				@Override public void setValue(byte[] value) { set(value,0,value.length); }

				@Override
				public boolean matchingValue(byte[] item) {
            throw new UnsupportedOperationException("IMPLEMENT");
//						return Bytes.equals(value,offset,length,item,0,item.length);
				}

				@Override public byte[] getValue() { return value; }

				@Override
				public boolean equals(Object o) {
						if (this == o) return true;
						if (!(o instanceof BytesElement)) return false;

						BytesElement that = (BytesElement) o;

            return byteComparator.equals(value,this.offset,this.length,that.value,that.offset,that.length);
				}

				@Override public int hashCode() {
            return hashFunction.hash(value,offset,length);
        }

				private void set(byte[] bytes, int offset, int length){
						this.value = bytes;
						this.offset = offset;
						this.length = length;
				}

				public boolean matchingValue(byte[] bytes, int offset, int length) {
            return byteComparator.equals(value,this.offset,this.length,bytes,offset,length);
				}
		}
}
