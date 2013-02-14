package com.splicemachine.si.coprocessor;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.log4j.Logger;
import com.splicemachine.constants.TxnConstants.TableEnv;
import com.splicemachine.impl.si.txn.Transaction;
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
		super.preGet(e, get, results);
	}

	private void validateCommitTimestamp (ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL, long commitTimestamp) throws IOException {
			if (put.getTimeStamp() > commitTimestamp) {
				super.prePut(e, put, edit, writeToWAL);
				return;
			}
			throw new IOException(String.format(SIConstants.WRITE_WRITE_CONFLICT_COMMIT, put,commitTimestamp));		
	}

	private void recursiveWriteWriteCheck(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL,long topTimestamp) throws IOException {
		Get get = new Get(put.getRow());
		get.setMaxVersions(1);
		get.setTimeRange(0,topTimestamp);
		Result result;
		if ( (result = region.get(get, null)) == null || result.isEmpty()) { // No Matching Record, Write It... // Should use Bloom Filter?
			super.prePut(e, put, edit, writeToWAL);
			return;
		}
		SpliceLogUtils.trace(LOG, "result= %s",result);
		KeyValue commitTimestampValue = result.getColumnLatest(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES, SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN);
		if (commitTimestampValue != null) {
			validateCommitTimestamp(e,put,edit,writeToWAL,commitTimestampValue.getTimestamp());
		} else { // Commit Timestamp Miss...
			KeyValue startTimestampValue = result.getColumnLatest(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES, SIConstants.SNAPSHOT_ISOLATION_RECORD_COLUMN);
			Transaction transaction = Transaction.readTransaction(startTimestampValue.getTimestamp());
			switch (transaction.getTransactionState()) {
			case COMMIT:
				validateCommitTimestamp(e,put,edit,writeToWAL,transaction.getCommitTimestamp());
			case ACTIVE:
				throw new IOException(String.format(SIConstants.WRITE_WRITE_CONFLICT_COMMIT, put,transaction.getStartTimestamp()));								
			case ABORT:
				recursiveWriteWriteCheck(e,put,edit,writeToWAL,transaction.getStartTimestamp());
				break;
			case ERROR:
				recursiveWriteWriteCheck(e,put,edit,writeToWAL,transaction.getStartTimestamp());
				break;
			default:
				throw new IOException(String.format(SIConstants.NO_TRANSACTION_STATUS,transaction));
			}		
		}
	}
	
	@Override
	public void prePut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, boolean writeToWAL) throws IOException {
		if (!this.tableEnvMatch) {
			super.prePut(e,put,edit,writeToWAL);
			return;
		}
		SpliceLogUtils.trace(LOG, "prePut on table %s, %s", e.getEnvironment().getRegion().getTableDesc().getNameAsString(),put);
		recursiveWriteWriteCheck(e,put,edit,writeToWAL,Long.MAX_VALUE);
	}

	@Override
	public void preDelete(ObserverContext<RegionCoprocessorEnvironment> e, Delete delete, WALEdit edit, boolean writeToWAL) throws IOException {
		if (!this.tableEnvMatch) {
			super.preDelete(e,delete,edit,writeToWAL);
			return;
		}
		SpliceLogUtils.trace(LOG, "preDelete on table %s, %s", e.getEnvironment().getRegion().getTableDesc().getNameAsString(),delete);
		// Write - Write Conflict Detection
		// tombstone ?
		super.preDelete(e, delete, edit, writeToWAL);
	}

}
