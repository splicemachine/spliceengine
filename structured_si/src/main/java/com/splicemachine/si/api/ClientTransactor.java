package com.splicemachine.si.api;

import com.splicemachine.si.impl.TransactionId;

import java.io.IOException;

/**
 * Transaction capabilities exposed to client processes (i.e. they don't have direct access to the transaction store).
 */
public interface ClientTransactor<PutOp, GetOp, ScanOp, MutationOp> {
    TransactionId transactionIdFromString(String transactionId);
    TransactionId transactionIdFromGet(GetOp get);
    TransactionId transactionIdFromScan(ScanOp scan);
    TransactionId transactionIdFromPut(PutOp put);

    void initializeGet(String transactionId, GetOp get) throws IOException;
    void initializeGet(String transactionId, GetOp get, boolean includeSIColumn) throws IOException;
    void initializeScan(String transactionId, ScanOp scan);
    void initializeScan(String transactionId, ScanOp scan, boolean includeSIColumn, boolean includeUncommittedAsOfStart);
    void initializePut(String transactionId, PutOp put);

    PutOp createDeletePut(TransactionId transactionId, Object rowKey);
    boolean isDeletePut(MutationOp put);
}
