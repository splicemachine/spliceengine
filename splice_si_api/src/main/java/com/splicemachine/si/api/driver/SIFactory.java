package com.splicemachine.si.api.driver;

import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.data.*;
import com.splicemachine.si.api.filter.RowAccumulator;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.*;
import com.splicemachine.si.api.txn.Txn.IsolationLevel;
import com.splicemachine.si.api.txn.Txn.State;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.region.RegionTxnStore;
import com.splicemachine.si.impl.region.TransactionResolver;
import com.splicemachine.si.impl.store.IgnoreTxnCacheSupplier;
import com.splicemachine.storage.EntryAccumulator;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.stream.StreamException;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.timestamp.impl.TimestampServer;

import java.io.IOException;
import java.util.List;

public interface SIFactory<OperationWithAttributes,Data,Delete extends OperationWithAttributes,Filter,
        Get extends OperationWithAttributes,Mutation,OperationStatus,Pair,Put extends OperationWithAttributes,Region,
        RegionScanner,Result,ReturnCode,RowLock,Scan extends OperationWithAttributes,Table,TableBuffer,Transaction> {
	public RowAccumulator<Data> getRowAccumulator(EntryPredicateFilter predicateFilter, EntryDecoder decoder, boolean countStar);
	public RowAccumulator<Data> getRowAccumulator(EntryPredicateFilter predicateFilter, EntryDecoder decoder, EntryAccumulator accumulator, boolean countStar);
    public STableWriter<Delete,Mutation,OperationStatus,Pair,Put,Table> getTableWriter();
    public SDataLib<OperationWithAttributes,Data,Delete,Filter,Get,
            Put,RegionScanner,Result,Scan> getDataLib();
    public STransactionLib<Transaction,TableBuffer> getTransactionLib();
    public DataStore<OperationWithAttributes,Data,Delete,Filter,
            Get,Mutation,OperationStatus,
            Put,RegionScanner,Result,ReturnCode,RowLock,Scan,Table> getDataStore();
    public STableReader<Table, Get, Scan,Result> getTableReader();
    public TxnStore getTxnStore();
    public TxnSupplier getTxnSupplier();
    public IgnoreTxnCacheSupplier getIgnoreTxnCacheSupplier();
    public SOperationStatusLib<OperationStatus> getOperationStatusLib();
    public TxnOperationFactory getTxnOperationFactory();
    public SReturnCodeLib<ReturnCode> getReturnCodeLib();
    public TransactionalRegion getTransactionalRegion(Region region);
	Transaction getTransaction(long txnId, long beginTimestamp, long parentTxnId,
                               long commitTimestamp, long globalCommitTimestamp,
                               boolean hasAdditiveField, boolean additive,
                               IsolationLevel isolationLevel, State state, String destTableBuffer);
	void storeTransaction(RegionTxnStore regionTransactionStore, Transaction transaction) throws IOException;
	long getTxnId(Transaction transaction);
	byte[] transactionToByteArray(MultiFieldEncoder mfe, Transaction transaction);
	TxnView transform(List<Data> element) throws StreamException;
    long getTransactionTimeout();
    int getTranactionLockStripes();
    int getActiveTransactionCacheSize();
    int getCompletedTransactionCacheSize();
    int getCompletedTransactionConcurrency();
    int getTransactionKeepAliveThreads();
    int getRollForwardRate();
    int getReadResolverThreads();
    int getReadResolverQueueSize();
    TransactionReadController<Data,Filter,Get,Result,ReturnCode,Scan> getTransactionReadController();
    TimestampSource getTimestampSource();
    TimestampServer getTimestampServer();
    KeepAliveScheduler getKeepAliveScheduler();
    SExceptionLib getExceptionLib();
    TransactionResolver getTransactionResolver();
}