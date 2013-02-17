package com.splicemachine.si.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;
import com.splicemachine.constants.TxnConstants.TableEnv;
import com.splicemachine.impl.si.txn.Transaction;
import com.splicemachine.si.filters.SIFilter;
import com.splicemachine.si.utils.SIConstants;
import com.splicemachine.si.utils.SIUtils;
import com.splicemachine.utils.SpliceLogUtils;

public class SIObserver extends BaseRegionObserver {
	private static Logger LOG = Logger.getLogger(SIObserver.class);
	protected HRegion region;
	protected static HTablePool tablePool = new HTablePool();
	protected static ConcurrentHashMap<Long,Transaction> cache = new ConcurrentHashMap<Long,Transaction>();
	private boolean tableEnvMatch = false;	
	@Override
	public void start(CoprocessorEnvironment e) throws IOException {
		SpliceLogUtils.trace(LOG, "starting %s",SIObserver.class);
		region = ((RegionCoprocessorEnvironment) e).getRegion();
		tableEnvMatch = SIUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TableEnv.USER_TABLE) 
				|| SIUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TableEnv.USER_INDEX_TABLE)
				|| SIUtils.getTableEnv((RegionCoprocessorEnvironment) e).equals(TableEnv.DERBY_SYS_TABLE);
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
		if (tableEnvMatch) {
			if (get.getFilter() != null)
				get.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,get.getFilter(),new SIFilter(get.getTimeRange().getMax(),region))); // Wrap Existing Filters
			else
				get.setFilter(new SIFilter(get.getTimeRange().getMax(),region));				
		}
		super.preGet(e, get, results);
	}

	@Override
	public RegionScanner preScannerOpen(ObserverContext<RegionCoprocessorEnvironment> e, Scan scan,RegionScanner s) throws IOException {
		SpliceLogUtils.trace(LOG, "preScannerOpen %s", scan);		
		if (tableEnvMatch) {
			if (scan.getFilter() != null)
				scan.setFilter(new FilterList(FilterList.Operator.MUST_PASS_ALL,scan.getFilter(),new SIFilter(scan.getTimeRange().getMax(),region))); // Wrap Existing Filters
			else
				scan.setFilter(new SIFilter(scan.getTimeRange().getMax(),region));				
		}
		return super.preScannerOpen(e, scan, s);
	}

	private void validateCommitTimestamp (ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL, long commitTimestamp) throws IOException {
		if (put.getTimeStamp() > commitTimestamp) {
			super.prePut(e, put, edit, writeToWAL);
			return;
		}
		throw new IOException(String.format(SIConstants.WRITE_WRITE_CONFLICT_COMMIT, put,commitTimestamp));		
	}

	private boolean hasConflict (long mutationBeginTimestamp, long lastCommitTimestamp) {
			if (lastCommitTimestamp > mutationBeginTimestamp)
				return true;
			return false;
	}
	
	private boolean hasWriteWriteConflict(byte[] row, long mutationBeginTimestamp, long topTimestamp) throws IOException {
		Get get = new Get(row);
		get.setMaxVersions(1);
		get.setTimeRange(0,topTimestamp);
		get.addFamily(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES);
		Result result;
		if ( (result = region.get(get, null)) == null || result.isEmpty()) // No Matching Record, Write It... // Validate this uses Bloom (i.e. no disk I/O)
			return false;
		SpliceLogUtils.trace(LOG, "result= %s",result);
		KeyValue commitTimestampValue = result.getColumnLatest(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES, SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN);
		if (commitTimestampValue.getValue() != SIConstants.ZERO_BYTE_ARRAY) {
			return hasConflict(mutationBeginTimestamp,commitTimestampValue.getTimestamp());
		} else { // Commit Timestamp Miss...
			Transaction transaction = Transaction.readTransaction(commitTimestampValue.getTimestamp());
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
		if (tableEnvMatch && hasWriteWriteConflict(put.getRow(),put.getTimeStamp(),Long.MAX_VALUE))
			throw new IOException(String.format(SIConstants.WRITE_WRITE_CONFLICT_COMMIT,put));
		super.prePut(e,put,edit,writeToWAL);
	}

	@Override
	public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, boolean writeToWAL) throws IOException {
		SpliceLogUtils.trace(LOG, "preDelete on table %s, %s", e.getEnvironment().getRegion().getTableDesc().getNameAsString(),delete);
		if (tableEnvMatch) { 
			Put put = new Put(delete.getRow(),delete.getTimeStamp());
			put.add(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN, SIConstants.ZERO_BYTE_ARRAY);
			region.put(put, writeToWAL);
			return;
		}
		super.preDelete(e, delete, edit, writeToWAL);
	}

}
