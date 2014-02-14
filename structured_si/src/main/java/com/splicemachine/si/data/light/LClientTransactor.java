package com.splicemachine.si.data.light;

import com.splicemachine.si.api.ClientTransactor;
import com.splicemachine.si.api.TransactionManager;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.TransactionId;
import org.apache.hadoop.hbase.client.OperationWithAttributes;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 2/13/14
 */
@SuppressWarnings("unchecked")
public class LClientTransactor<
				Put extends OperationWithAttributes,
				Get extends OperationWithAttributes,
				Scan extends OperationWithAttributes,
				Mutation extends OperationWithAttributes>
				implements ClientTransactor<Put,Get,Scan,Mutation> {
		private final DataStore dataStore;
		private final TransactionManager control;
		private final SDataLib dataLib;

		public LClientTransactor(DataStore dataStore,
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
		public void initializeGet(String transactionId, Get get) throws IOException {
				initializeOperation(transactionId,get,true);
		}

		@Override
		public void initializeScan(String transactionId, Scan scan, boolean includeSIColumn) {
				initializeOperation(transactionId,scan,includeSIColumn);
		}

		@Override
		public void initializePut(String transactionId, Put put) {
				initializePut(transactionId, put,true);
		}

		@Override
		public void initializePut(String transactionId, Put put, boolean addPlaceHolderColumnToEmptyPut) {
				initializeOperation(transactionId,put,false);
				if(addPlaceHolderColumnToEmptyPut)
						dataStore.addPlaceHolderColumnToEmptyPut(put);
		}

		@Override
		public Put createDeletePut(TransactionId transactionId, byte[] rowKey) {
				return createDeletePutDirect(transactionId.getId(),rowKey);
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

		/**
		 * Create a "put" operation that will effectively delete a given row.
		 */
		private Put createDeletePutDirect(long transactionId, byte[] rowKey) {
				final Put deletePut = (Put) dataLib.newPut(rowKey);
				flagForSITreatment(transactionId, false, deletePut);
				dataStore.setTombstoneOnPut(deletePut, transactionId);
				dataStore.setDeletePutAttribute(deletePut);
				return deletePut;
		}

		private void initializeOperation(String transactionId, OperationWithAttributes operation, boolean includeSIColumn) {
				flagForSITreatment(control.transactionIdFromString(transactionId).getId(), includeSIColumn, operation);
		}

		/**
		 * Set an attribute on the operation that identifies it as needing "snapshot isolation" treatment. This is so that
		 * later when the operation comes through for processing we will know how to handle it.
		 */
		private void flagForSITreatment(long transactionId, boolean includeSIColumn,
																		OperationWithAttributes operation) {
				dataStore.setSINeededAttribute(operation, includeSIColumn);
				dataStore.setTransactionId(transactionId, operation);
		}
}
