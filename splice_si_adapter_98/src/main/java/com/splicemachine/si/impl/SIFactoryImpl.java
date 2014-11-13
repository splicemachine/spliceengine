package com.splicemachine.si.impl;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import com.google.protobuf.ByteString;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.table.BetterHTablePool;
import com.splicemachine.hbase.table.SpliceHTableFactory;
import com.splicemachine.si.api.RowAccumulator;
import com.splicemachine.si.api.SIFactory;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnStore;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.Txn.IsolationLevel;
import com.splicemachine.si.api.Txn.State;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.coprocessor.TxnMessage.Txn;
import com.splicemachine.si.coprocessor.TxnMessage.TxnInfo;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.data.hbase.HPoolTableSource;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.data.hbase.HTableReader;
import com.splicemachine.si.data.hbase.HTableWriter;
import com.splicemachine.si.impl.region.HTransactionLib;
import com.splicemachine.si.impl.region.STransactionLib;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.utils.ByteSlice;

public class SIFactoryImpl implements SIFactory<TxnMessage.TxnInfo> {
	
	public static final SDataLib dataLib = new HDataLib();
	public static final STableWriter tableWriter = new HTableWriter();
	public static final STransactionLib transactionLib = new HTransactionLib();

	

	@Override
	public RowAccumulator getRowAccumulator(EntryPredicateFilter predicateFilter, EntryDecoder decoder,
			boolean countStar) {
		return new HRowAccumulator(dataLib,predicateFilter,decoder,countStar);
	}

	@Override
	public RowAccumulator getRowAccumulator(EntryPredicateFilter predicateFilter, EntryDecoder decoder,
			EntryAccumulator accumulator, boolean countStar) {
		return new HRowAccumulator(dataLib,predicateFilter,decoder,accumulator,countStar);
	}

	@Override
	public STableWriter getTableWriter() {
		return tableWriter;
	}

	@Override
	public SDataLib getDataLib() {
		return dataLib;
	}


	@Override
	public DataStore getDataStore() {
			return new DataStore(getDataLib(), getTableReader(),getTableWriter(),
									SIConstants.SI_NEEDED,
                SIConstants.SI_DELETE_PUT,
									SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,
									SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,
									HConstants.EMPTY_BYTE_ARRAY,
									SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES,
									SIConstants.SNAPSHOT_ISOLATION_FAILED_TIMESTAMP,
									SIConstants.DEFAULT_FAMILY_BYTES,
									TransactionStorage.getTxnSupplier(),
									TransactionLifecycle.getLifecycleManager()
									);
	}

	@Override
	public STableReader getTableReader() {
		BetterHTablePool hTablePool = new BetterHTablePool(new SpliceHTableFactory(),
				SpliceConstants.tablePoolCleanerInterval, TimeUnit.SECONDS,
				SpliceConstants.tablePoolMaxSize,SpliceConstants.tablePoolCoreSize);
		final HPoolTableSource tableSource = new HPoolTableSource(hTablePool);
		final STableReader reader;
		try {
			return new HTableReader(tableSource);
		} catch (IOException e) {
			throw new RuntimeException(e);
	}

	}

	@Override
	public TxnStore getTxnStore() {
		return TransactionStorage.getTxnStore();
	}

	@Override
	public TxnSupplier getTxnSupplier() {
		return TransactionStorage.getTxnSupplier();
	}

	@Override
	public TransactionalRegion getTransactionalRegion(HRegion region) {
		return TransactionalRegions.get(region);
	}

	@Override
	public STransactionLib getTransactionLib() {
		return transactionLib;
	}

	@Override
	public TxnMessage.TxnInfo getTransaction(long txnId, long beginTimestamp,
			long parentTxnId, long commitTimestamp,
			long globalCommitTimestamp, boolean hasAdditiveField,
			boolean additive, IsolationLevel isolationLevel, State state,
			String destTableBuffer) {
		return TxnMessage.TxnInfo.newBuilder().setTxnId(txnId).setBeginTs(beginTimestamp)
		.setParentTxnid(parentTxnId).setDestinationTables(ByteString.copyFrom(Bytes.toBytes(destTableBuffer))).setIsolationLevel(isolationLevel.encode()).build();
	}
}
