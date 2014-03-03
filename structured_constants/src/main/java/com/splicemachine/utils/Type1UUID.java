package com.splicemachine.utils;

import com.splicemachine.constants.bytes.BytesUtil;
import org.apache.hadoop.hbase.util.Bytes;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Scott Fines
 *         Date: 3/3/14
 */
public class Type1UUID {
		private static final long IP_ADDRESS_INT;
		private static final int JVM_UNIQUE_INT = (int)(System.currentTimeMillis()>>>8);
		private static final long highBits;
		static{
				byte[] addr;
				try{
						addr = InetAddress.getLocalHost().getAddress();
				} catch (UnknownHostException e) {
						addr = new byte[4];
				}

				IP_ADDRESS_INT = (long) Bytes.toInt(addr);

				long bits = IP_ADDRESS_INT<<32;
				bits |= JVM_UNIQUE_INT;
				highBits = bits;
		}

		private long lastTimestamp = System.currentTimeMillis();
		private int counter;

		private static final Type1UUID INSTANCE = new Type1UUID();

		private Type1UUID() { }

		public static Generator newGenerator(int bufferSize){
				return new Generator(INSTANCE,bufferSize);
		}

		public long[] nextUUID(){
				long[] lowBits = new long[1];
				next(lowBits);
				return new long[]{highBits,lowBits[0]};
		}

		public void next(long[] buffer){
				int numRecords = buffer.length;
				long timestamp;
				int startCount,stopCount;
				synchronized (this){
						timestamp = System.currentTimeMillis();
						if(timestamp==lastTimestamp){
								if(counter==0){
										while(timestamp==lastTimestamp){
												LockSupport.parkNanos(1000*100);
												timestamp = System.currentTimeMillis();
										}
								}
						}
						if(numRecords> Short.MAX_VALUE-counter+1){
							numRecords = Short.MAX_VALUE-counter+1;
						}
						startCount = counter;
						counter+=numRecords;
						stopCount=counter;
						counter = counter & Short.MAX_VALUE;
						lastTimestamp = timestamp;
				}

				int pos=0;
				for(int i=startCount;i< stopCount;i++,pos++){
						buffer[pos]	= buildUUIDLowBits(timestamp, i);
				}
				if(pos<buffer.length)
						Arrays.fill(buffer, stopCount-startCount,buffer.length,-1l);
		}

		private long buildUUIDLowBits(long timestamp, int count) {
				long lowBits = ((long)((short)(timestamp>>>32)))<<48;
				lowBits|=((long)((int)(timestamp)))<<32;
				lowBits |=((short)count);
				return lowBits;
		}

		public static class Generator implements UUIDGenerator{
				private final long[] lowBitsBuffer;
				private int currentPosition;
				private final Type1UUID generator;

				private Generator(Type1UUID generator,int bufferSize) {
						this.lowBitsBuffer = new long[bufferSize];
						this.generator = generator;
						this.currentPosition=bufferSize+1;
				}

				@Override
				public byte[] nextBytes() {
						byte[] data = new byte[encodedLength()];
						next(data,0);
						return data;
				}

				@Override public int encodedLength() { return 16; }

				@Override
				public void next(byte[] data, int offset) {
						if(currentPosition>=lowBitsBuffer.length||lowBitsBuffer[currentPosition]==-1l){
								generator.next(lowBitsBuffer);
								currentPosition=0;
						}
						int pos = currentPosition;
						currentPosition++;

						long lowBits = lowBitsBuffer[pos];
						toBytes(lowBits,data,offset);
				}

				private void toBytes(long lowBits, byte[] data, int offset) {
						BytesUtil.longToBytes(highBits,data,offset);
						offset+=8;
						BytesUtil.longToBytes(lowBits,data,offset);
				}
		}
}
