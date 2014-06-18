package com.splicemachine.si.data.api;

import java.io.IOException;

import com.splicemachine.si.api.Txn;
import org.apache.hadoop.hbase.client.OperationWithAttributes;
import com.splicemachine.si.api.ClientTransactor;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.TransactionId;

public abstract class AbstractClientTransactor<Put extends OperationWithAttributes,Get extends OperationWithAttributes,
				Scan extends OperationWithAttributes,Mutation extends OperationWithAttributes>implements ClientTransactor<Put,Get,Scan,Mutation> {
		protected final DataStore dataStore;
		protected final TransactionManager control;
		protected final SDataLib dataLib;

		public AbstractClientTransactor(DataStore dataStore,
																		TransactionManager control,
																		SDataLib dataLib) {
				this.dataStore = dataStore;
				this.control = control;
				this.dataLib = dataLib;
		}

		@Override
		public TransactionId transactionIdFromGet(Get get) {
				return dataStore.getTransactionIdFromOperation(get);
		}

		@Override
		public TransactionId transactionIdFromScan(Scan scan) {
				return dataStore.getTransactionIdFromOperation(scan);
		}

		@Override
		public TransactionId transactionIdFromPut(Put put) {
				return dataStore.getTransactionIdFromOperation(put);
		}

		@Override
		public long txnIdFromPut(Put put) {
				return dataStore.getTxnIdFromOp(put);
		}

		@Override
		public void initializeGet(String transactionId, Get get) throws IOException {
				initializeOperation(transactionId,get);
		}

		@Override
		public void initializeGet(long txnId, Get get) throws IOException {
				initializeOperation(txnId,get);
		}

		@Override
		public void initializeScan(String transactionId, Scan scan) {
				initializeOperation(transactionId,scan);
		}

		@Override
		public void initializeScan(Txn txn, Scan scan) {
				initializeOperation(txn,scan);
		}

		@Override
		public void initializePut(String transactionId, Put put) {
				initializeOperation(transactionId, put);
		}

		@Override
		public void initializePut(long txnId, Put put) {
				initializeOperation(txnId,put);
		}

		@Override
		public Put createDeletePut(TransactionId transactionId, byte[] rowKey) {
				return createDeletePutDirect(transactionId.getId(),rowKey);
		}

		@Override
		public Put createDeletePut(Txn txn, byte[] rowKey) {
				return createDeletePutDirect(txn.getTxnId(),rowKey);
		}

		@Override
		public boolean isDeletePut(Mutation put) {
				final Boolean deleteAttribute = dataStore.getDeletePutAttribute(put);
				return (deleteAttribute != null && deleteAttribute);
		}

		@Override
		public boolean requiresSI(Put put) {
				return dataStore.getSINeededAttribute(put)!=null;
		}

		protected void initializeOperation(String transactionId, OperationWithAttributes operation) {
				flagForSITreatment(control.transactionIdFromString(transactionId).getId(), operation);
		}

		protected void initializeOperation(Txn txn, OperationWithAttributes operation) {
				flagForSITreatment(txn.getTxnId(), operation);
		}

		protected void initializeOperation(long txnId, OperationWithAttributes operation) {
				flagForSITreatment(txnId, operation);
		}

		protected void flagForSITreatment(long transactionId, OperationWithAttributes operation) {
				dataStore.setSINeededAttribute(operation);
				dataStore.setTransactionId(transactionId, operation);
		}

		protected Put createDeletePutDirect(long transactionId, byte[] rowKey) {
				final Put deletePut = (Put) dataLib.newPut(rowKey);
				flagForSITreatment(transactionId, deletePut);
				dataStore.setTombstoneOnPut(deletePut, transactionId);
				dataStore.setDeletePutAttribute(deletePut);
				return deletePut;
		}
}
