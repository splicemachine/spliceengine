package com.splicemachine.si2.si.api;

import com.splicemachine.si2.data.api.SGet;
import com.splicemachine.si2.data.api.SScan;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

import java.util.List;

/**
 * Transaction capabilities exposed to client processes (i.e. they don't have direct access to the transaction store).
 */
public interface ClientTransactor {
    TransactionId transactionIdFromString(String transactionId);
    TransactionId getTransactionIdFromPut(Object put);
    TransactionId getTransactionIdFromDelete(Delete delete);

    void initializeGet(TransactionId transactionId, SGet get);
    void initializeGets(TransactionId transactionId, List gets);
    void initializeScan(TransactionId transactionId, SScan scan);
    void initializePut(TransactionId transactionId, Object put);

    /**
     * Flag put2 for the same kind of transaction treatment as put1 has.
     */
    void initializePut(Object put1, Object put2);
    void initializeGetFromDelete(Object put1, Object put2);

    Object newDeletePut(TransactionId transactionId, Object rowKey);
    boolean isDeletePut(Object put);
}
