package com.splicemachine.si.api;

import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.Txn.IsolationLevel;
import com.splicemachine.si.api.Txn.State;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.data.api.STableReader;
import com.splicemachine.si.data.api.STableWriter;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.region.RegionTxnStore;
import com.splicemachine.si.impl.region.STransactionLib;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import org.apache.hadoop.hbase.regionserver.HRegion;

import java.io.IOException;

public interface SIFactory<Transaction> {
	RowAccumulator getRowAccumulator(EntryPredicateFilter predicateFilter,EntryDecoder decoder,boolean countStar);
	STableWriter getTableWriter();
    SDataLib getDataLib();
    STransactionLib getTransactionLib();
    DataStore getDataStore();
    STableReader getTableReader();
    TxnStore getTxnStore();
    TxnSupplier getTxnSupplier();
    IgnoreTxnCacheSupplier getIgnoreTxnSupplier();
    TransactionalRegion getTransactionalRegion(HRegion region);
	Transaction getTransaction(long txnId, long beginTimestamp, long parentTxnId,
			long commitTimestamp, long globalCommitTimestamp,
			boolean hasAdditiveField, boolean additive,
			IsolationLevel isolationLevel, State state, String destTableBuffer);
	void storeTransaction(RegionTxnStore regionTransactionStore, Transaction transaction) throws IOException;
	long getTxnId(Transaction transaction);
	byte[] transactionToByteArray(MultiFieldEncoder mfe, Transaction transaction);
}