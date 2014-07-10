package com.splicemachine.si.impl;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.si.api.ReadOnlyModificationException;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnOperationFactory;
import com.splicemachine.si.api.TxnSupplier;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 7/8/14
 */
public class SimpleOperationFactory implements TxnOperationFactory {

		private final TxnSupplier txnSupplier;

		public SimpleOperationFactory(TxnSupplier txnSupplier) {
				this.txnSupplier = txnSupplier;
		}

		@Override
		public Put newPut(Txn txn,byte[] rowKey) throws ReadOnlyModificationException {
				Put put = new Put(rowKey);
//				if(txn==null) {
//						makeNonTransactional(put);
//						return put;
//				}
        if(!txn.allowsWrites())
            throw new ReadOnlyModificationException("transaction is read only: "+ txn.getTxnId());
				encodeForWrites(put, txn);
				return put;
		}


		@Override
		public Scan newScan(Txn txn) {
				return newScan(txn,false);
		}

		@Override
		public Scan newScan(Txn txn, boolean isCountStar) {
				Scan scan = new Scan();
				if(txn==null) {
						makeNonTransactional(scan);
						return scan;
				}
				encodeForReads(scan,txn,isCountStar);
				return scan;
		}

		@Override
		public Get newGet(Txn txn,byte[] rowKey) {
				Get get = new Get(rowKey);
				if(txn==null){
						makeNonTransactional(get);
						return get;
				}
				encodeForReads(get,txn,false);
				return get;
		}

		@Override
		public Mutation newDelete(Txn txn,byte[] rowKey) throws ReadOnlyModificationException {
        if(txn==null){
            Delete delete = new Delete(rowKey);
            makeNonTransactional(delete);
            return delete;
        }

				Put delete = new Put(rowKey);
				delete.setAttribute(SIConstants.SI_DELETE_PUT,SIConstants.TRUE_BYTES);
        delete.add(SpliceConstants.DEFAULT_FAMILY_BYTES,
										SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,txn.getTxnId(), HConstants.EMPTY_BYTE_ARRAY);
        if(!txn.allowsWrites())
            throw new ReadOnlyModificationException("transaction is read only: "+ txn.getTxnId());
        encodeForWrites(delete,txn);
				return delete;
		}

		@Override
		public Txn fromReads(OperationWithAttributes op) throws IOException {
				byte[] txnData = op.getAttribute(SIConstants.SI_TRANSACTION_ID_KEY);
				if(txnData==null) return null; //non-transactional
				long beginTs = Bytes.toLong(txnData,0,8);
        long parentTxnId = Bytes.toLong(txnData,8,8);
				Txn.IsolationLevel isoLevel = Txn.IsolationLevel.fromByte(txnData[16]);
        Txn parentTxn;
        if(parentTxnId<0 || parentTxnId==beginTs){
            //we are effectively a read-only child of the parent
            parentTxn = Txn.ROOT_TRANSACTION;
        }else
            parentTxn = txnSupplier.getTransaction(parentTxnId);
				return new ReadOnlyTxn(beginTs,beginTs,isoLevel,parentTxn,UnsupportedLifecycleManager.INSTANCE,false,false);
		}

		@Override
		public Txn fromWrites(OperationWithAttributes op) throws IOException {
				byte[] txnData = op.getAttribute(SIConstants.SI_TRANSACTION_ID_KEY);
				if(txnData==null) return null; //non-transactional
				long txnId = Bytes.toLong(txnData,0,8);
				long parentTxnId = Bytes.toLong(txnData,8,8);
				boolean isAdditive = BytesUtil.toBoolean(txnData,16); //TODO -sf- add this in somehow
        Txn parentTxn = parentTxnId<0? Txn.ROOT_TRANSACTION : txnSupplier.getTransaction(parentTxnId);
				return new ActiveWriteTxn(txnId,txnId,parentTxn,isAdditive);
		}

		/******************************************************************************************************************/
		/*private helper functions*/
		private void encodeForWrites(OperationWithAttributes op, Txn txn) {
				byte[] data = new byte[17];
				BytesUtil.longToBytes(txn.getTxnId(),data,0);
				BytesUtil.longToBytes(txn.getParentTransaction().getTxnId(),data,8);
				data[16] = (byte)(txn.isAdditive()? -1: 0);
				op.setAttribute(SIConstants.SI_TRANSACTION_ID_KEY,data);
        op.setAttribute(SIConstants.SI_NEEDED,SIConstants.SI_NEEDED_VALUE_BYTES);
		}

		private void encodeForReads(OperationWithAttributes op, Txn txn,boolean isCountStar) {
				if(isCountStar)
						op.setAttribute(SIConstants.SI_COUNT_STAR,SIConstants.TRUE_BYTES);

				byte[] data = new byte[17];
				BytesUtil.longToBytes(txn.getBeginTimestamp(),data,0);
        BytesUtil.longToBytes(txn.getParentTransaction().getTxnId(),data,8);
				data[16] = txn.getIsolationLevel().encode();

				op.setAttribute(SIConstants.SI_TRANSACTION_ID_KEY,data);
        op.setAttribute(SIConstants.SI_NEEDED,SIConstants.SI_NEEDED_VALUE_BYTES);
		}


		private void makeNonTransactional(OperationWithAttributes op) {
				op.setAttribute(SIConstants.SI_EXEMPT,SIConstants.TRUE_BYTES);
		}
}
