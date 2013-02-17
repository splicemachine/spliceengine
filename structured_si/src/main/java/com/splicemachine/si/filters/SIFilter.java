package com.splicemachine.si.filters;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.impl.si.txn.Transaction;
import com.splicemachine.si.utils.SIConstants;
import com.splicemachine.utils.SpliceLogUtils;

public class SIFilter extends FilterBase {
	private static Logger LOG = Logger.getLogger(SIFilter.class);
	protected long startTimestamp;
	protected byte[] currentRow;
	protected Long currentTombstoneMarker;
	protected HashMap<Long,Long> committedTransactions;
	protected HRegion region;
	protected byte[] lastValidQualifier = SIConstants.ZERO_BYTE_ARRAY;
	
	public SIFilter() {
		
	}
	/**
	 * Client side filter without commit roll forward...
	 * Used for unit testing purposes
	 * 
	 * @param startTimestamp
	 */
	public SIFilter(long startTimestamp) {
		this.startTimestamp = startTimestamp;
	}
	/**
	 * Server side filter wrapped into other filters on the coprocessor side.
	 * 
	 * @param startTimestamp
	 * @param region
	 */
	public SIFilter(long startTimestamp, HRegion region) {
		this.startTimestamp = startTimestamp;
		this.region = region;
	}
	
	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(startTimestamp);		
	}
	
	private void cleanupErrors (KeyValue keyValue) {
		if (region == null)
			return;
		Delete delete = new Delete(keyValue.getRow());
		delete.setTimestamp(keyValue.getTimestamp());
		try {
			region.delete(delete, null, false);
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, e);
		}		
	}

	private void rollCommitForward (KeyValue keyValue, Transaction transaction) {
		if (region == null)
			return;
		Put put = new Put(keyValue.getRow(),keyValue.getTimestamp());
		put.add(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES, SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN, Bytes.toBytes(transaction.getCommitTimestamp()));
		try {
			region.put(put, false);
		} catch (Exception e) {
			SpliceLogUtils.logAndThrowRuntime(LOG, e);
		}		
	}
	
	private void updateCommitTimestamps (KeyValue keyValue) {
 		byte[] commitValue = keyValue.getValue();
 		if (Arrays.equals(commitValue,SIConstants.ZERO_BYTE_ARRAY)) {
			Transaction transaction = Transaction.readTransaction(keyValue.getTimestamp());
			switch (transaction.getTransactionState()) {
			case ABORT:
			case ERROR: // Cleanup Errors, Not Durable but Fast.
				cleanupErrors(keyValue);			
				return;
			case ACTIVE: // Ignore active transactional data
				return; 
			case COMMIT: // Roll Forward, Not Durable but fast
				rollCommitForward(keyValue,transaction);
				if (keyValue.getTimestamp() < startTimestamp)
					committedTransactions.put(keyValue.getTimestamp(), transaction.getCommitTimestamp());
				return;
			default:
				SpliceLogUtils.logAndThrow(LOG, new RuntimeException("Transaction status not set - should not happen"));		
			}			
		} else {
			if (Bytes.toLong(commitValue) < startTimestamp) {
				committedTransactions.put(keyValue.getTimestamp(), Bytes.toLong(commitValue));
			}
		}
	}
	
	private void rowReset(KeyValue keyValue) {
		currentRow = keyValue.getRow();
		currentTombstoneMarker = null;
		committedTransactions = new HashMap<Long,Long>();
	}
	
	public static boolean isCommitTimestamp(KeyValue keyValue) {
		return Arrays.equals(keyValue.getFamily(), SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES) && 
				Arrays.equals(keyValue.getQualifier(), SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN);
	}
	
	public static boolean isTombstone(KeyValue keyValue) {
		return Arrays.equals(keyValue.getFamily(), SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES) && 
				Arrays.equals(keyValue.getQualifier(), SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN);
	}
	
	
	@Override
	public ReturnCode filterKeyValue(KeyValue keyValue) {
		SpliceLogUtils.trace(LOG, "filterKeyValue %s",keyValue);
		if (currentRow == null || !Arrays.equals(currentRow,keyValue.getRow()))
			rowReset(keyValue);
		if (isCommitTimestamp(keyValue)) {
				updateCommitTimestamps(keyValue);
				return ReturnCode.SKIP;
		}
		if (isTombstone(keyValue)) {
			if (currentTombstoneMarker == null) {
				if (committedTransactions.containsKey(keyValue.getTimestamp()) || startTimestamp == keyValue.getTimestamp()) {
					currentTombstoneMarker = keyValue.getTimestamp();
					return ReturnCode.NEXT_COL;
				}
				return ReturnCode.SKIP;
			}
			throw new RuntimeException(SIConstants.FILTER_CHECKING_MULTIPLE_ROW_TOMBSTONES);
		}
		if (Arrays.equals(keyValue.getFamily(),SIConstants.DEFAULT_FAMILY) && 
			!Arrays.equals(lastValidQualifier,keyValue.getQualifier())) {
				if (committedTransactions.containsKey(keyValue.getTimestamp()) || 
					startTimestamp == keyValue.getTimestamp()) {
					if (keyValue.getValue() != null && Arrays.equals(keyValue.getValue(),SIConstants.ZERO_BYTE_ARRAY)) { 
						return ReturnCode.NEXT_COL;
					}
					if (currentTombstoneMarker == null || currentTombstoneMarker < keyValue.getTimestamp()) {
						lastValidQualifier = keyValue.getQualifier();
						return ReturnCode.INCLUDE;
					}
			} 
		}
		return ReturnCode.SKIP;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		startTimestamp = in.readLong();
	}

}
