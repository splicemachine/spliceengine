package com.splicemachine.si.api.driver;

import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.data.*;
import com.splicemachine.si.api.filter.RowAccumulator;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.*;
import com.splicemachine.si.api.txn.Txn.IsolationLevel;
import com.splicemachine.si.api.txn.Txn.State;
import com.splicemachine.si.api.txn.lifecycle.TxnPartition;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.impl.DataStore;
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
        Get extends OperationWithAttributes,OperationStatus,Put extends OperationWithAttributes,Region,
        RegionScanner,Result,ReturnCode,Scan extends OperationWithAttributes,TableBuffer>{
    RowAccumulator<Data> getRowAccumulator(EntryPredicateFilter predicateFilter,EntryDecoder decoder,boolean countStar);

    RowAccumulator<Data> getRowAccumulator(EntryPredicateFilter predicateFilter,EntryDecoder decoder,EntryAccumulator accumulator,boolean countStar);

    SDataLib<OperationWithAttributes, Data, Delete, Filter, Get,
            Put, RegionScanner, Result, Scan> getDataLib();

    STransactionLib<TxnMessage.Txn, TableBuffer> getTransactionLib();

    DataStore<OperationWithAttributes, Data, Delete, Filter,
            Get,
            Put, RegionScanner, Result, Scan> getDataStore();

    TxnStore getTxnStore();

    TxnSupplier getTxnSupplier();

    IgnoreTxnCacheSupplier getIgnoreTxnCacheSupplier();

    OperationStatusFactory getOperationStatusLib();

    TxnOperationFactory getTxnOperationFactory();

    TransactionalRegion getTransactionalRegion(Region region);

    TxnMessage.Txn getTransaction(long txnId,long beginTimestamp,long parentTxnId,
                               long commitTimestamp,long globalCommitTimestamp,
                               boolean hasAdditiveField,boolean additive,
                               IsolationLevel isolationLevel,State state,String destTableBuffer);

    void storeTransaction(TxnPartition regionTransactionStore,TxnMessage.Txn transaction) throws IOException;

    long getTxnId(TxnMessage.Txn transaction);

    byte[] transactionToByteArray(MultiFieldEncoder mfe,TxnMessage.Txn transaction);

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

    TransactionReadController<Data, Get, ReturnCode, Scan> getTransactionReadController();

    TimestampSource getTimestampSource();

    TimestampServer getTimestampServer();

    KeepAliveScheduler getKeepAliveScheduler();

    ExceptionFactory getExceptionLib();

    TransactionResolver getTransactionResolver();
}