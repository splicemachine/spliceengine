package com.splicemachine.si.coprocessor;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.splicemachine.constants.TxnConstants.TableEnv;
import com.splicemachine.iapi.txn.TransactionState;
import com.splicemachine.impl.si.txn.Transaction;
import com.splicemachine.si.filters.SIFilter;
import com.splicemachine.si.utils.SIConstants;
import com.splicemachine.si.utils.SIUtils;
import com.splicemachine.utils.SpliceLogUtils;

public class SIObserver extends BaseRegionObserver {
	private static Logger LOG = Logger.getLogger(SIObserver.class);
	protected HRegion region;
	protected static HTablePool tablePool = new HTablePool();
	protected static Cache<Long,Transaction> transactionalCache;
	protected Cache<Long,ArrayList<byte[]>> transactionRowCallbackCache;	
	private boolean tableEnvMatch = false;
	private boolean isTransactionalTable = false;
	static {
		transactionalCache = CacheBuilder.newBuilder().maximumSize(50000).expireAfterWrite(5, TimeUnit.MINUTES).removalListener(new RemovalListener<Long,Transaction>() {
			@Override
			public void onRemoval(RemovalNotification<Long, Transaction> notification) {
				SpliceLogUtils.trace(LOG, "transaction %s removed from transactional cache ",notification.getValue());
			}
		}).build();
		
	}
	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		SpliceLogUtils.trace(LOG, "starting %s",SIObserver.class);
		region = ((RegionCoprocessorEnvironment) e).getRegion();
		tableEnvMatch = SIUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TableEnv.USER_TABLE) 
				|| SIUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TableEnv.USER_INDEX_TABLE)
				|| SIUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TableEnv.DERBY_SYS_TABLE);
		if (tableEnvMatch) {
			transactionRowCallbackCache = CacheBuilder.newBuilder().maximumSize(50000).expireAfterWrite(100, TimeUnit.SECONDS).removalListener(new RemovalListener<Long,ArrayList<byte[]>>() {
				@Override
				public void onRemoval(RemovalNotification<Long, ArrayList<byte[]>> notification) {
					SpliceLogUtils.trace(LOG, "transaction %s removed from transactional cache ",notification.getValue());
				}
			
			}).build();
		}		
		super.start(e);
	}

	@Override
	public void stop(CoprocessorEnvironment e) throws IOException {
		SpliceLogUtils.trace(LOG, "stopping %s",SIObserver.class);
		super.stop(e);
	}

	@Override
	public void preGet(ObserverContext<RegionCoprocessorEnvironment> e, Get get, List<KeyValue> results) throws IOException {
		SpliceLogUtils.trace(LOG, "preGet %s", get);
		if (tableEnvMatch && SIUtils.shouldUseSI(get)) {
			if (get.getFilter() != null)
				get.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,get.getFilter(),new SIFilter(get.getTimeRange().getMax(),e.getEnvironment().getRegion(),transactionalCache))); // Wrap Existing Filters
			else
				get.setFilter(new SIFilter(get.getTimeRange().getMax(),e.getEnvironment().getRegion(),transactionalCache));				
		}
		super.preGet(e, get, results);
	}

	@Override
	public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan,RegionScanner s) throws IOException {
		SpliceLogUtils.trace(LOG, "preScannerOpen %s", scan);		
		if (tableEnvMatch && SIUtils.shouldUseSI(scan)) {
			if (scan.getFilter() != null)
				scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,scan.getFilter(),new SIFilter(scan.getTimeRange().getMax(),e.getEnvironment().getRegion(),transactionalCache))); // Wrap Existing Filters
			else
				scan.setFilter(new SIFilter(scan.getTimeRange().getMax(),e.getEnvironment().getRegion(),transactionalCache));				
		}
		return super.preScannerOpen(e, scan, s);
	}


	private boolean hasConflict (long mutationBeginTimestamp, long lastCommitTimestamp) {
			if (lastCommitTimestamp > mutationBeginTimestamp)
				return true;
			return false;
	}
	
	private boolean hasWriteWriteConflict(byte[] row, long mutationBeginTimestamp, long topTimestamp) throws IOException {		
		SpliceLogUtils.trace(LOG, "hasWriteWriteConflict with row %s using mutationTimestamp %d and topTimestamp %d",row, mutationBeginTimestamp, topTimestamp);
		Get get = new Get(row);
		get.setMaxVersions(1);
		get.setTimeRange(0,topTimestamp);
		get.addFamily(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES);
		Result result;
		if ( (result = region.get(get, null)) == null || result.isEmpty()) { // No Matching Record, Write It... // Validate this uses Bloom (i.e. no disk I/O)
			SpliceLogUtils.trace(LOG, "write-write-miss %s",mutationBeginTimestamp);
			return false;
		}
		SpliceLogUtils.trace(LOG, "result= %s",result);
		KeyValue commitTimestampValue = result.getColumnLatest(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES, SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES);
		if (!Arrays.equals(commitTimestampValue.getValue(), SIConstants.EMPTY_BYTE_ARRAY)) {
			SpliceLogUtils.trace(LOG, "hasCommitTimestamp");
			return hasConflict(mutationBeginTimestamp,commitTimestampValue.getTimestamp());
		} else { // Commit Timestamp Miss...
			Transaction transaction = Transaction.readTransaction(commitTimestampValue.getTimestamp(),transactionalCache);
			SpliceLogUtils.trace(LOG, "write-writehastrans %s",transaction);			
			switch (transaction.getTransactionState()) {
			case COMMIT:
				return hasConflict(mutationBeginTimestamp,transaction.getCommitTimestamp());
			case ACTIVE:
				return true;								
			case ABORT:
				return hasWriteWriteConflict(row,mutationBeginTimestamp,transaction.getStartTimestamp());
			case ERROR:
				return hasWriteWriteConflict(row,mutationBeginTimestamp,transaction.getStartTimestamp());
			default:
				throw new IOException(String.format(SIConstants.NO_TRANSACTION_STATUS,transaction));
			}		
		}
	}
	
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {
		SpliceLogUtils.trace(LOG, "prePut on table %s, %s", e.getEnvironment().getRegion().getTableDesc().getNameAsString(),put);
		if (tableEnvMatch && SIUtils.shouldUseSI(put)) { // Snapshot Isolation Table
			if (hasWriteWriteConflict(put.getRow(),put.getTimeStamp(),Long.MAX_VALUE)) { // Has Write - Write Conflict, throw DoNotRetry error
				e.complete();
				throw new DoNotRetryIOException(String.format(SIConstants.WRITE_WRITE_CONFLICT_COMMIT,put)); // Special Exception
			} else {
				ArrayList<byte[]> rows;
				if ( (rows = transactionRowCallbackCache.getIfPresent(put.getTimeStamp())) != null) {
					if (rows.size() < 10000) {
						rows.add(put.getRow());
					}
				} else {
					rows = new ArrayList<byte[]>();
					rows.add(put.getRow());
					transactionRowCallbackCache.put(put.getTimeStamp(), rows);
				}
			}
		}
		super.prePut(e,put,edit,writeToWAL);
	}

	@Override
	public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {
		SpliceLogUtils.trace(LOG, "postPut on table %s, %s", e.getEnvironment().getRegion().getTableDesc().getNameAsString(),put);
		if (isTransactionalTable) { // TXN Table must be after put so we do not jump the gun in case of failure
			Map<byte[],List<KeyValue>> familyMap = put.getFamilyMap();
			List<KeyValue> keyValues = familyMap.get(SIConstants.DEFAULT_FAMILY_BYTES);
			SITransactionResponse transactionResponse = new SITransactionResponse();
			for (KeyValue kv: keyValues) {
				if (Arrays.equals(kv.getQualifier(),SIConstants.TRANSACTION_START_TIMESTAMP_COLUMN_BYTES))
					transactionResponse.setStartTimestamp(Bytes.toLong(kv.getValue()));
				if (Arrays.equals(kv.getQualifier(),SIConstants.TRANSACTION_COMMIT_TIMESTAMP_COLUMN_BYTES) && Arrays.equals(kv.getValue(),SIConstants.EMPTY_BYTE_ARRAY))
					transactionResponse.setCommitTimestamp(Bytes.toLong(kv.getValue()));
				if (Arrays.equals(kv.getQualifier(),SIConstants.TRANSACTION_STATUS_COLUMN_BYTES))
					transactionResponse.setTransactionState(TransactionState.values()[Bytes.toInt(kv.getValue())]);						
			}
			
		}
		super.postPut(e,put,edit,writeToWAL);
	}

	
	@Override
	public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, boolean writeToWAL) throws IOException {
		SpliceLogUtils.trace(LOG, "preDelete on table %s, %s", e.getEnvironment().getRegion().getTableDesc().getNameAsString(),delete);
		if (tableEnvMatch && SIUtils.shouldUseSI(delete)) { 
			if (hasWriteWriteConflict(delete.getRow(),delete.getTimeStamp(),Long.MAX_VALUE)) {
				e.complete();
				throw new DoNotRetryIOException(String.format(SIConstants.WRITE_WRITE_CONFLICT_COMMIT,delete)); // Special Exception
			} else {
				Put put = new Put(delete.getRow(),delete.getTimeStamp());
				put.add(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES, SIConstants.EMPTY_BYTE_ARRAY);
				region.put(put, writeToWAL);
				ArrayList<byte[]> rows;
				if ( (rows = transactionRowCallbackCache.getIfPresent(put.getTimeStamp())) != null) {
					if (rows.size() < 10000)
						rows.add(put.getRow());
				} else {
					rows = new ArrayList<byte[]>();
					rows.add(put.getRow());
					transactionRowCallbackCache.put(put.getTimeStamp(), rows);
				}
				return;
			}
		}
		super.preDelete(e, delete, edit, writeToWAL);
	}

	   @Override
	   public InternalScanner preCompact(ObserverContext<RegionCoprocessorEnvironment> e, Store store, InternalScanner scanner) {
			SpliceLogUtils.trace(LOG, "preCompact on table %s", e.getEnvironment().getRegion().getTableDesc().getNameAsString());
	      if (tableEnvMatch)
	    	  return new SICompactionScanner(scanner,transactionalCache);
	      return scanner;
	   }

	public Cache<Long, ArrayList<byte[]>> getTransactionRowCallbackCache() {
		return transactionRowCallbackCache;
	}

	public void setTransactionRowCallbackCache(Cache<Long, ArrayList<byte[]>> transactionRowCallbackCache) {
		this.transactionRowCallbackCache = transactionRowCallbackCache;
	}
	   
	   

}
