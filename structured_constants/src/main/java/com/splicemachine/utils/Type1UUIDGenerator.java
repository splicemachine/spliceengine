package com.splicemachine.utils;

import org.apache.hadoop.hbase.util.Bytes;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * @author Scott Fines
 *         Date: 2/27/14
 */
public class Type1UUIDGenerator implements UUIDGenerator {
		private static final UUIDGenerator INSTANCE = new Type1UUIDGenerator();
		private static final byte[] IP_ADDRESS;
		private static final byte[] JVM_UNIQUE = Bytes.toBytes((int)(System.currentTimeMillis() >>> 8));
		static{
				byte[] addr;
				try{
						addr = InetAddress.getLocalHost().getAddress();
				} catch (UnknownHostException e) {
						addr = new byte[4];
				}
				IP_ADDRESS = addr;
		}

		private final AtomicInteger counter = new AtomicInteger(0);

		private Type1UUIDGenerator(){}// singleton pattern

		public static UUIDGenerator instance(){
				return INSTANCE;
		}

		@Override
		public byte[] nextBytes() {
				byte[] bytes = new byte[encodedLength()];
				next(bytes,0);
				return bytes;
		}

		@Override public int encodedLength() { return 16; }

		@Override
		public void next(byte[] data, int offset) {

//				II II II II JJ JJ JJ JJ HH HH LL LL LL LL CC CC
				System.arraycopy(IP_ADDRESS,0,data,offset,IP_ADDRESS.length);
				offset+=IP_ADDRESS.length;
				System.arraycopy(JVM_UNIQUE,0,data,offset,JVM_UNIQUE.length);
				offset+=JVM_UNIQUE.length;

				//Get the timestamp and the counter value, waiting if necessary for a counter to become available
				long timestamp;
				short count;
				boolean shouldContinue;
				do{
						timestamp = System.currentTimeMillis();
						int possibleCount = counter.get();
						if(possibleCount<=0){
								//we've rolled our counter--wait for a new timestamp
								long newTs = timestamp;
								while(newTs==timestamp){
										LockSupport.parkNanos(1000*100); //up to 10 parks in a ms, each 100 microseconds in length
										newTs = System.currentTimeMillis();
								}
								timestamp = newTs;
						}
						count = (short)possibleCount;
						int nextPossible = (possibleCount+1) & Short.MAX_VALUE;
						shouldContinue = counter.compareAndSet(possibleCount,nextPossible);
				}while(!shouldContinue);

				byte[] timeHigh = Bytes.toBytes((short)(timestamp >>>32));
				System.arraycopy(timeHigh,0,data,offset,timeHigh.length);
				offset+=timeHigh.length;
				byte[] timeLow = Bytes.toBytes((int)timestamp);
				System.arraycopy(timeLow,0,data,offset,timeLow.length);
				offset+=timeLow.length;
				byte[] countBytes = Bytes.toBytes(count);
				System.arraycopy(countBytes,0,data,offset,countBytes.length);
		}

}
