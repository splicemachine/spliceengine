package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongArrayList;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.si.api.*;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.*;

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
		public Put newPut(TxnView txn,byte[] rowKey) throws ReadOnlyModificationException {
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
		public Scan newScan(TxnView txn) {
				return newScan(txn,false);
		}

		@Override
		public Scan newScan(TxnView txn, boolean isCountStar) {
				Scan scan = new Scan();
				if(txn==null) {
						makeNonTransactional(scan);
						return scan;
				}
				encodeForReads(scan,txn,isCountStar);
				return scan;
		}

		@Override
		public Get newGet(TxnView txn,byte[] rowKey) {
				Get get = new Get(rowKey);
				if(txn==null){
						makeNonTransactional(get);
						return get;
				}
				encodeForReads(get,txn,false);
				return get;
		}

		@Override
		public Mutation newDelete(TxnView txn,byte[] rowKey) throws ReadOnlyModificationException {
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
		public TxnView fromReads(OperationWithAttributes op) throws IOException {
				byte[] txnData = op.getAttribute(SIConstants.SI_TRANSACTION_ID_KEY);
				if(txnData==null) return null; //non-transactional
        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(txnData);
				long beginTs = decoder.decodeNextLong();
        boolean additive = decoder.decodeNextBoolean();
        Txn.IsolationLevel level = Txn.IsolationLevel.fromByte(decoder.decodeNextByte());

        TxnView parent = Txn.ROOT_TRANSACTION;
        while(decoder.available()){
            long id = decoder.decodeNextLong();
            parent = new ReadOnlyTxn(id,id,level,parent,UnsupportedLifecycleManager.INSTANCE,additive);
        }
				return new ReadOnlyTxn(beginTs,beginTs,level,parent,UnsupportedLifecycleManager.INSTANCE,additive);
		}

		@Override
		public TxnView fromWrites(OperationWithAttributes op) throws IOException {
				byte[] txnData = op.getAttribute(SIConstants.SI_TRANSACTION_ID_KEY);
				if(txnData==null) return null; //non-transactional
        MultiFieldDecoder decoder = MultiFieldDecoder.wrap(txnData);
        long beginTs = decoder.decodeNextLong();
        boolean additive = decoder.decodeNextBoolean();
        Txn.IsolationLevel level = Txn.IsolationLevel.fromByte(decoder.decodeNextByte());

        TxnView parent = Txn.ROOT_TRANSACTION;
        while(decoder.available()){
            long id = decoder.decodeNextLong();
            parent = new ActiveWriteTxn(id,id,parent,additive,level);
        }
        return new ActiveWriteTxn(beginTs,beginTs,parent,additive,level);
		}

		/******************************************************************************************************************/
		/*private helper functions*/
		private void encodeForWrites(OperationWithAttributes op, TxnView txn) {

        byte[] data =encodeTransaction(txn);
				op.setAttribute(SIConstants.SI_TRANSACTION_ID_KEY,data);
        op.setAttribute(SIConstants.SI_NEEDED,SIConstants.SI_NEEDED_VALUE_BYTES);
		}

		private void encodeForReads(OperationWithAttributes op, TxnView txn,boolean isCountStar) {
				if(isCountStar)
						op.setAttribute(SIConstants.SI_COUNT_STAR,SIConstants.TRUE_BYTES);

        byte[] data = encodeTransaction(txn);

				op.setAttribute(SIConstants.SI_TRANSACTION_ID_KEY,data);
        op.setAttribute(SIConstants.SI_NEEDED,SIConstants.SI_NEEDED_VALUE_BYTES);
		}

    private byte[] encodeTransaction(TxnView txn) {
        MultiFieldEncoder encoder = MultiFieldEncoder.create(4)
                .encodeNext(txn.getTxnId())
                .encodeNext(txn.isAdditive())
                .encodeNext(txn.getIsolationLevel().encode());

        LongArrayList parentTxnIds = LongArrayList.newInstance();
        byte[] build = encodeParentIds(txn, parentTxnIds);
        encoder.setRawBytes(build);
        return encoder.build();
    }

    private byte[] encodeParentIds(TxnView txn, LongArrayList parentTxnIds) {
        /*
         * For both active reads AND active writes, we only need to know the
         * parent's transaction ids, since we'll use the information immediately
         * available to determine other properties (additivity, etc.) Thus,
         * by doing this bit of logic, we can avoid a network call on the server
         * for every parent on the transaction chain, at the cost of 2-10 bytes
         * per parent on the chain--a cheap trade.
         */
        TxnView parent = txn.getParentTxnView();
        while(!Txn.ROOT_TRANSACTION.equals(parent)){
            parentTxnIds.add(parent.getTxnId());
            parent = parent.getParentTxnView();
        }
        int parentSize = parentTxnIds.size();
        long[] parentIds = parentTxnIds.buffer;
        MultiFieldEncoder parents = MultiFieldEncoder.create(parentSize);
        for(int i=1;i<parentSize;i++){
            parents.encodeNext(parentIds[parentSize-i]);
        }
        return parents.build();
    }


    private void makeNonTransactional(OperationWithAttributes op) {
				op.setAttribute(SIConstants.SI_EXEMPT,SIConstants.TRUE_BYTES);
		}
}
