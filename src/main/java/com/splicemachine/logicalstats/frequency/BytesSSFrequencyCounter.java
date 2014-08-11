package com.splicemachine.logicalstats.frequency;

import com.splicemachine.utils.hash.Hash32;
import org.apache.hadoop.hbase.util.Bytes;

import java.nio.ByteBuffer;

/**
 * @author Scott Fines
 *         Date: 3/26/14
 */
class BytesSSFrequencyCounter extends SSFrequencyCounter<byte[]> implements BytesFrequencyCounter {
		protected BytesSSFrequencyCounter(int maxSize, int initialCapacity, Hash32[] hashFunctions) {
				super(maxSize, initialCapacity, hashFunctions);
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
				BytesElement staleElement = null;
				int visitedCount = 0;
				HASH_LOOP:for (Hash32 hashFunction : hashFunctions) {
						int pos = hashFunction.hash(bytes,offset,length) & (capacity - 1);
						for(int i=0;i<3 && visitedCount<capacity;i++){
								int newPos = (pos+i) & (capacity-1);
								BytesElement existing = (BytesElement)hashtable[newPos];
								if (existing == null) {
										if(staleElement==null){
												existing = new BytesElement();
												hashtable[newPos] = existing;
												staleElement = existing;
										}
										break HASH_LOOP;
								}if (existing.stale) {
										if(staleElement==null)
												staleElement = existing;
								}else if (existing.matchingValue(bytes,offset,length)) {
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
						staleElement.set(bytes,offset,length);
						increment(staleElement,count);
						staleElement.stale = false;
						elementsSeen++;
				}else{
						expandCapacity();
						update(bytes,offset,length);
				}
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

		private class BytesElement extends Element{
				private byte[] value;
				private int offset;
				private int length;

				@Override public void setValue(byte[] value) { set(value,0,value.length); }

				@Override
				public boolean matchingValue(byte[] item) {
						return Bytes.equals(value,offset,length,item,0,item.length);
				}

				@Override public byte[] value() { return value; }

				@Override
				public boolean equals(Object o) {
						if (this == o) return true;
						if (!(o instanceof BytesElement)) return false;

						BytesElement that = (BytesElement) o;

						return Bytes.equals(value,offset,length,that.value,that.offset,that.length);
				}

				@Override public int hashCode() { return Bytes.hashCode(value,offset,length); }

				private void set(byte[] bytes, int offset, int length){
						this.value = bytes;
						this.offset = offset;
						this.length = length;
				}

				public boolean matchingValue(byte[] bytes, int offset, int length) {
						return Bytes.equals(value,offset,length,bytes,offset,length);
				}
		}
}
