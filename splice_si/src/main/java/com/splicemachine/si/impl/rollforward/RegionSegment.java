package com.splicemachine.si.impl.rollforward;

import com.splicemachine.hash.HashFunctions;
import com.splicemachine.stats.cardinality.ConcurrentHyperLogLogCounter;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.util.Bytes;
import org.cliffc.high_scale_lib.Counter;

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
	private final ConcurrentHyperLogLogCounter txnCounter =  new ConcurrentHyperLogLogCounter(6,HashFunctions.murmur2_64(0));
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
}
