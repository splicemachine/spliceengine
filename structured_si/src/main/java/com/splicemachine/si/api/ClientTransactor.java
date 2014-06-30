package com.splicemachine.si.api;

import com.splicemachine.si.impl.TransactionId;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import org.apache.hadoop.hbase.client.Scan;

import java.io.IOException;

/**
 * Transaction capabilities exposed to client processes (i.e. they don't have direct access to the transaction store)
 * for constructing operations to be applied under transaction control.
 */
public interface ClientTransactor<Put extends OperationWithAttributes, Get, Scan, Mutation> {
    TransactionId transactionIdFromGet(Get get);
    TransactionId transactionIdFromScan(Scan scan);
    TransactionId transactionIdFromPut(Put put);

		void initializeGet(String transactionId, Get get) throws IOException;

		void initializeScan(String transactionId, Scan scan);

		void initializePut(String transactionId, Put put);

		Put createDeletePut(TransactionId transactionId, byte[] rowKey);
		Put createDeletePut(Txn txn, byte[] rowKey);
    boolean isDeletePut(Mutation put);
	boolean requiresSI(Put put);

		Txn txnFromOp(OperationWithAttributes op, boolean readOnly) throws IOException;

		void initializeOperation(Txn txn, OperationWithAttributes op) throws IOException;
}
