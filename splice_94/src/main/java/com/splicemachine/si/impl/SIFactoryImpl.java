package com.splicemachine.si.impl;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.table.BetterHTablePool;
import com.splicemachine.hbase.table.SpliceHTableFactory;
import com.splicemachine.si.api.RowAccumulator;
import com.splicemachine.si.api.SIFactory;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.TxnStore;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.data.hbase.HDataLib;
import com.splicemachine.si.data.hbase.HPoolTableSource;
import com.splicemachine.si.data.hbase.HRowAccumulator;
import com.splicemachine.si.data.hbase.HTableReader;
import com.splicemachine.si.data.hbase.HTableWriter;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;

public class SIFactoryImpl implements SIFactory {

	@Override
	public RowAccumulator getRowAccumulator(
			EntryPredicateFilter predicateFilter, EntryDecoder decoder,
			boolean countStar) {
		return new HRowAccumulator(predicateFilter,decoder,countStar);
	}

	@Override
	public RowAccumulator getRowAccumulator(
			EntryPredicateFilter predicateFilter, EntryDecoder decoder,
			EntryAccumulator accumulator, boolean countStar) {
		return new HRowAccumulator(predicateFilter,decoder,accumulator,countStar);
	}

	@Override
	public STableWriter getTableWriter() {
		return new HTableWriter();
	}

	@Override
	public SDataLib getDataLib() {
		return new HDataLib();
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

}
