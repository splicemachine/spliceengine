package com.splicemachine.si2.si.api;

import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.SScan;

import java.util.List;

/**
 * Transaction capabilities exposed to client processes (i.e. they don't have direct access to the transaction store).
 */
public interface ClientTransactor {
    void initializeGet(TransactionId transactionId, SGet get);
    void initializeGets(TransactionId transactionId, List gets);
    void initializeScan(TransactionId transactionId, SScan scan);
    void initializePut(TransactionId transactionId, Object put);
}
