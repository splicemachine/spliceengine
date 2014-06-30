package com.splicemachine.si.data.api;

import com.splicemachine.si.api.ClientTransactor;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.TransactionId;
import org.apache.hadoop.hbase.client.OperationWithAttributes;

import java.io.IOException;

public abstract class AbstractClientTransactor<Put extends OperationWithAttributes,Get extends OperationWithAttributes,
				Scan extends OperationWithAttributes,Mutation extends OperationWithAttributes>implements ClientTransactor<Put,Get,Scan,Mutation> {
		protected final DataStore dataStore;
//		protected final TransactionManager control;
		protected final TxnLifecycleManager control;
		protected final SDataLib dataLib;

		public AbstractClientTransactor(DataStore dataStore,
																		TxnLifecycleManager control,
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
		public Txn txnFromOp(OperationWithAttributes op, boolean readOnly) throws IOException {
				return dataStore.getTxn(op,readOnly);
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
		public void initializeGet(String transactionId, Get get) throws IOException {
				initializeOperation(transactionId,get);
		}

		@Override
		public void initializeScan(String transactionId, Scan scan) {
				initializeOperation(transactionId,scan);
		}

		@Override
		public void initializePut(String transactionId, Put put) {
				initializeOperation(transactionId, put);
		}

		@Override
		public Put createDeletePut(TransactionId transactionId, byte[] rowKey) {
				throw new UnsupportedOperationException("REMOVE");
//				return createDeletePutDirect(transactionId.getId(),rowKey);
		}

		@Override
		public Put createDeletePut(Txn txn, byte[] rowKey) {
				return createDeletePutDirect(txn,rowKey);
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
				throw new UnsupportedOperationException("IMPLEMENT");
//				flagForSITreatment(control.transactionIdFromString(transactionId).getId(), operation);
		}

		public void initializeOperation(Txn txn, OperationWithAttributes operation) {
				flagForSITreatment(txn, operation);
		}

		protected void flagForSITreatment(Txn txn, OperationWithAttributes operation) {
				dataStore.setSINeededAttribute(operation);
				dataStore.setTransaction(txn, operation);
		}

		protected Put createDeletePutDirect(Txn txn, byte[] rowKey) {
				final Put deletePut = (Put) dataLib.newPut(rowKey);
				flagForSITreatment(txn, deletePut);
				dataStore.setTombstoneOnPut(deletePut, txn.getTxnId());
				dataStore.setDeletePutAttribute(deletePut);
				return deletePut;
		}

}
