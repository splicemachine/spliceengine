package com.splicemachine.si.impl;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.si.api.ClientTransactor;
import com.splicemachine.si.api.TransactionManager;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Arrays;

/**
 * @author Scott Fines
 * Date: 2/13/14
 */
public class HBaseClientTransactor implements ClientTransactor<Put,Get,Scan,Mutation,byte[]>{
		private static final byte[] TRUE_BYTES = Bytes.toBytes(true);
		private static final byte[] FALSE_BYTES = Bytes.toBytes(false);
		private final TransactionManager transactionControl;

		public HBaseClientTransactor(TransactionManager transactionControl) {
				this.transactionControl = transactionControl;
		}

		@Override public TransactionId transactionIdFromGet(Get get) { return getTransactionId(get); }
		@Override public TransactionId transactionIdFromScan(Scan scan) { return getTransactionId(scan); }
		@Override public TransactionId transactionIdFromPut(Put put) { return getTransactionId(put); }
		@Override public void initializeGet(String transactionId, Get get) throws IOException { initializeOperation(transactionId, get,true); }
		@Override public void initializeScan(String transactionId, Scan scan, boolean includeSIColumn) { initializeOperation(transactionId,scan,includeSIColumn);  }
		@Override public void initializePut(String transactionId, Put put) {initializePut(transactionId, put,true);  }

		@Override
		public void initializePut(String transactionId, Put put, boolean addPlaceHolderColumnToEmptyPut) {
				initializeOperation(transactionId,put,false);
				if(addPlaceHolderColumnToEmptyPut && put.getFamilyMap()==null||put.getFamilyMap().size()<=0){
						put.add(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES,
										SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,
										0l, SIConstants.EMPTY_BYTE_ARRAY);
				}
		}

		@Override
		public Put createDeletePut(TransactionId transactionId, byte[] rowKey) {
				Put put = new Put(rowKey);
				setSI(put,false,transactionId.getId());
				put.add(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES,
								Bytes.toBytes(SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_STRING),
								transactionId.getId(),SIConstants.EMPTY_BYTE_ARRAY);
				put.setAttribute(SIConstants.SI_DELETE_PUT, TRUE_BYTES);
				return put;
		}

		@Override
		public boolean isDeletePut(Mutation put) {
				byte[] deletePut = put.getAttribute(SIConstants.SI_DELETE_PUT);
				return deletePut!=null && Arrays.equals(deletePut, TRUE_BYTES);
		}


/**********************************************************************************/
		/*private helper methods*/
		private TransactionId getTransactionId(OperationWithAttributes operation) {
				byte[] txnIdValue = operation.getAttribute(SIConstants.SI_TRANSACTION_ID_KEY);
				if(txnIdValue!=null){
						return new TransactionId(Bytes.toString(txnIdValue));
				}
				return null;
		}

		private void initializeOperation(String transactionId, OperationWithAttributes op,boolean includeSIColumn) {
				long txnId = transactionControl.transactionIdFromString(transactionId).getId();
				setSI(op, includeSIColumn, txnId);
		}

		private void setSI(OperationWithAttributes op, boolean includeSIColumn, long txnId) {
				op.setAttribute(SIConstants.SI_NEEDED, includeSIColumn ? TRUE_BYTES : FALSE_BYTES);
				op.setAttribute(SIConstants.SI_TRANSACTION_ID_KEY, Bytes.toBytes(txnId));
		}
}
