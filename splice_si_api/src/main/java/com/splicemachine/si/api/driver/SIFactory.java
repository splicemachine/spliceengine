package com.splicemachine.si.api.driver;

import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.data.OperationStatusFactory;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.filter.TransactionReadController;
import com.splicemachine.si.api.server.TransactionalRegion;
import com.splicemachine.si.api.txn.Txn.IsolationLevel;
import com.splicemachine.si.api.txn.Txn.State;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.api.txn.lifecycle.TxnPartition;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.stream.StreamException;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.timestamp.impl.TimestampServer;

import java.io.IOException;
import java.util.List;

public interface SIFactory<OperationWithAttributes,Data,Delete extends OperationWithAttributes,Filter,
        Get extends OperationWithAttributes,Put extends OperationWithAttributes,Region,
        RegionScanner,Result,ReturnCode,Scan extends OperationWithAttributes>{

    SDataLib<OperationWithAttributes, Data, Delete, Filter, Get, Put, RegionScanner, Result, Scan> getDataLib();

    DataStore<OperationWithAttributes, Data, Delete, Filter, Get, Put, RegionScanner, Result, Scan> getDataStore();

    OperationStatusFactory getOperationStatusLib();

    TransactionalRegion getTransactionalRegion(Region region);

    TxnMessage.Txn getTransaction(long txnId,long beginTimestamp,long parentTxnId,
                               long commitTimestamp,long globalCommitTimestamp,
                               boolean hasAdditiveField,boolean additive,
                               IsolationLevel isolationLevel,State state,String destTableBuffer);

    void storeTransaction(TxnPartition regionTransactionStore,TxnMessage.Txn transaction) throws IOException;

    long getTxnId(TxnMessage.Txn transaction);

    TxnView transform(List<Data> element) throws StreamException;

    long getTransactionTimeout();

    TransactionReadController<Data, Get, ReturnCode, Scan> getTransactionReadController();

    TimestampSource getTimestampSource();

    TimestampServer getTimestampServer();

    ExceptionFactory getExceptionLib();

}