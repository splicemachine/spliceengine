package com.splicemachine.si.impl.rollforward;

import com.splicemachine.hash.Hash64;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.util.Bytes;
import org.cliffc.high_scale_lib.Counter;

import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 *
 * Represents a Segment of a Region. When Statistics are integrated, we can
 * replace many of the components of this class with cleaner implementations. In
 * the meantime, we steal the ConcurrentHyperLogLogCounter and use that.
 *
 * @author Scott Fines
 * Date: 6/26/14
 */
class RegionSegment {
		private final byte[] startKey;
		private final byte[] endKey;
		private final Counter toResolve = new Counter();
		private final ConcurrentHyperLogLogCounter txnCounter =  new ConcurrentHyperLogLogCounter(6);
		private volatile boolean inProgress;
    private String toString; //cache the toString for performance when needed

    RegionSegment(byte[] startKey, byte[] endKey) {
				this.startKey = startKey;
				this.endKey = endKey;
		}

		void update(long txnId,long rowsWritten){
				toResolve.add(rowsWritten);
				txnCounter.update(txnId);
		}

		long getToResolveCount(){
				return toResolve.estimate_get();
		}

		double estimateUniqueWritingTransactions(){
				return txnCounter.getEstimate();
		}

		byte[] getRangeStart(){
				return startKey;
		}

		byte[] getRangeEnd(){
				return endKey;
		}

		void reset(){
				txnCounter.clear();
		}

		void markInProgress(){
				inProgress=true;
		}

		void markCompleted(){
				inProgress=false;
		}

		public boolean isInProgress() {
				return inProgress;
		}

		public void rowResolved() {
				toResolve.decrement();
		}

    @Override
    public String toString() {
        if(toString==null)
            toString =  "["+Bytes.toStringBinary(startKey)+","+Bytes.toString(endKey)+")";
        return toString;
    }

    public int position(byte[] rowKey) {
				if(startKey.length<=0){
						if(endKey.length<=0) return 0;
						else{
								int compare = Bytes.compareTo(rowKey,endKey);
								if(compare>=0) return 1;
								return 0;
						}
				}
				int compare = Bytes.compareTo(startKey,rowKey);
				if(compare>0) //start key > row key
					return -1;
				else{
						if(endKey.length<=0) return 0;
						else{
								compare = Bytes.compareTo(rowKey,endKey);
								if(compare>=0) return 1;
								return 0;
						}
				}
		}

		public int position(ByteSlice rowKey) {
				if(startKey.length<=0){
						if(endKey.length<=0) return 0;
						else{
								int compare = rowKey.compareTo(endKey,0,endKey.length);
								if(compare>=0) return 1;
								return 0;
						}
				}
				int compare = rowKey.compareTo(startKey,0,startKey.length);
				if(compare<=0) return compare; //startKey > endKey
        else if(endKey.length<=0) return 0;
				else{
						compare = rowKey.compareTo(endKey,0,endKey.length);
						if(compare>=0) return 1;
						return 0;
				}
		}

		/*
				 * Private implementation of a HyperLogLogCounter which
				 * is concurrent. When we move Statistics into the system,
				 * we'll replace this implementation with the one provided
				 * by the Statistics engine, but until then, we'll use this
				 * version.
				 */
		private static class ConcurrentHyperLogLogCounter{
				private final int numRegisters;
				private final int precision;

				//cached for performance
				private final double alphaM;
				private final int shift;

				private final AtomicIntegerArray registers;

				private final ReadWriteLock readClearLock = new ReentrantReadWriteLock(false);
        private final Hash64 hashFunction = HashFunctions.murmur2_64(0);


				private ConcurrentHyperLogLogCounter(int precision) {
						this.precision = precision;
						this.numRegisters = 1<<precision;
						this.shift = Long.SIZE-precision;

						alphaM = computeAlpha(numRegisters);
						this.registers = new AtomicIntegerArray(numRegisters);
				}

				void update(long value){
						long hash = hashFunction.hash(value);
						int register = (int)(hash>>>shift);
						long w = hash << precision;
						int p = w==0l?1: (Long.numberOfLeadingZeros(w)+1);

						boolean success;
						do{
								int currVal = registers.get(register);
								if(currVal>=p) return; //nothing to update

								success = registers.compareAndSet(register,currVal,p);
						}while(!success);
				}

				double getEstimate(){
						Lock readLock = readClearLock.readLock();
						readLock.lock();
						try{
								double z = 0d;
								int zeroRegisterCount = 0;
								for(int i=0;i< numRegisters;i++){
										int value = registers.get(i);
										if(value==0)
												zeroRegisterCount++;
										z+=1d/(1<<value);
								}

								double E = alphaM*Math.pow(numRegisters,2)/z;
								if(E <= 5*numRegisters/2){
										E = numRegisters*Math.log((double)numRegisters/zeroRegisterCount);
								}
								return (long)E;
						}finally{
								readLock.unlock();
						}
				}

				void clear(){
					Lock writeLock = readClearLock.writeLock();
						writeLock.lock();
						try{
								for(int i=0;i<numRegisters;i++){
										registers.set(i,0); //reset the counter
								}
						}finally{
								writeLock.unlock();
						}
				}

				private double computeAlpha(int numRegisters) {
						switch(numRegisters){
								case 16: return 0.673d;
								case 32: return 0.697d;
								case 64: return 0.709d;
								default:
										return 0.7213/(1+1.079d/ numRegisters);
						}
				}
		}
}
