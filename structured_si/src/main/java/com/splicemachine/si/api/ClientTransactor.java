package com.splicemachine.si.api;

import com.splicemachine.si.impl.TransactionId;

import java.io.IOException;

/**
 * Transaction capabilities exposed to client processes (i.e. they don't have direct access to the transaction store)
 * for constructing operations to be applied under transaction control.
 */
public interface ClientTransactor<Put, Get, Scan, Mutation, Data> extends TransactorControl {
    TransactionId transactionIdFromGet(Get get);
    TransactionId transactionIdFromScan(Scan scan);
    TransactionId transactionIdFromPut(Put put);

    void initializeGet(String transactionId, Get get) throws IOException;
    void initializeScan(String transactionId, Scan scan, boolean includeSIColumn);
    void initializePut(String transactionId, Put put);
    void initializePut(String transactionId, Put put, boolean addPlaceHolderColumnToEmptyPut);
    Put createDeletePut(TransactionId transactionId, Data rowKey);
    boolean isDeletePut(Mutation put);
}
