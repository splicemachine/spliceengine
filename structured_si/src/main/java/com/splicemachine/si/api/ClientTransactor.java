package com.splicemachine.si.api;

import com.splicemachine.si.impl.TransactionId;
import org.apache.hadoop.hbase.client.OperationWithAttributes;

import java.io.IOException;

/**
 * Transaction capabilities exposed to client processes (i.e. they don't have direct access to the transaction store)
 * for constructing operations to be applied under transaction control.
 */
public interface ClientTransactor<Put extends OperationWithAttributes, Get, Scan, Mutation> {
    TransactionId transactionIdFromGet(Get get);
    TransactionId transactionIdFromScan(Scan scan);
    TransactionId transactionIdFromPut(Put put);
		long txnIdFromPut(Put put);
    void initializeGet(String transactionId, Get get) throws IOException;
		void initializeGet(long txnId, Get get) throws IOException;
    void initializeScan(String transactionId, Scan scan);
		void initializeScan(Txn txn, Scan scan);
    void initializePut(String transactionId, Put put);
		void initializePut(long txnId, Put put);
    Put createDeletePut(TransactionId transactionId, byte[] rowKey);
		Put createDeletePut(Txn txn, byte[] rowKey);
    boolean isDeletePut(Mutation put);
	boolean requiresSI(Put put);

}
