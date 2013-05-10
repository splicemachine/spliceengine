package com.splicemachine.si.api;

import com.splicemachine.si.data.api.SGet;
import com.splicemachine.si.data.api.SScan;
import org.apache.hadoop.hbase.client.Delete;

import java.io.IOException;

/**
 * Transaction capabilities exposed to client processes (i.e. they don't have direct access to the transaction store).
 */
public interface ClientTransactor {
    TransactionId transactionIdFromString(String transactionId);
    TransactionId transactionIdFromOperation(Object put);

    void initializeGet(String transactionId, SGet get) throws IOException;
    void initializeScan(String transactionId, SScan scan);
    void initializeScan(String transactionId, SScan scan, boolean siOnly);
    void initializePut(String transactionId, Object put);

    Object createDeletePut(TransactionId transactionId, Object rowKey);
    boolean isDeletePut(Object put);
}
