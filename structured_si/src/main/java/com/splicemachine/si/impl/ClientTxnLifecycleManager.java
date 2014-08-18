package com.splicemachine.si.impl;

import com.splicemachine.si.api.*;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.annotations.ThreadSafe;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Represents a Client Transaction Lifecycle Manager.
 *
 * This class makes decisions like when a ReadOnly transaction is created instead of a writeable,
 * when a transaction is recorded to the transaction table, and so on.
 *
 * @author Scott Fines
 * Date: 6/20/14
 */
@ThreadSafe
public class ClientTxnLifecycleManager implements TxnLifecycleManager {

		@ThreadSafe private final TimestampSource timestampSource;
		@ThreadSafe private TxnStore store;
		@ThreadSafe private KeepAliveScheduler keepAliveScheduler;

		public ClientTxnLifecycleManager(@ThreadSafe TimestampSource timestampSource) {
				this.timestampSource = timestampSource;
		}

		public void setStore(TxnStore store) {
				this.store = store;
		}


		public void setKeepAliveScheduler(KeepAliveScheduler scheduler){
				this.keepAliveScheduler = scheduler;
		}

		@Override
		public Txn beginTransaction() throws IOException {
				return beginTransaction(Txn.ROOT_TRANSACTION.getIsolationLevel());
		}

		@Override
		public Txn beginTransaction(byte[] destinationTable) throws IOException {
				return beginChildTransaction(Txn.ROOT_TRANSACTION,destinationTable);
		}

		@Override
		public Txn beginTransaction(Txn.IsolationLevel isolationLevel) throws IOException {
				return beginChildTransaction(Txn.ROOT_TRANSACTION,isolationLevel,null);
		}

		@Override
		public Txn beginTransaction(Txn.IsolationLevel isolationLevel, byte[] destinationTable) throws IOException {
				return beginChildTransaction(Txn.ROOT_TRANSACTION,isolationLevel,destinationTable);
		}

		@Override
		public Txn beginChildTransaction(TxnView parentTxn, byte[] destinationTable) throws IOException {
				if(parentTxn==null)
						parentTxn = Txn.ROOT_TRANSACTION;
				return beginChildTransaction(parentTxn,parentTxn.getIsolationLevel(),parentTxn.isDependent(),parentTxn.isAdditive(),destinationTable);
		}

		@Override
		public Txn beginChildTransaction(TxnView parentTxn, Txn.IsolationLevel isolationLevel, byte[] destinationTable) throws IOException {
				if(parentTxn==null)
						parentTxn = Txn.ROOT_TRANSACTION;
				return beginChildTransaction(parentTxn, isolationLevel, parentTxn.isDependent(), parentTxn.isAdditive(), destinationTable);
		}

		@Override
		public Txn beginChildTransaction(TxnView parentTxn, Txn.IsolationLevel isolationLevel, boolean dependent, byte[] destinationTable) throws IOException {
				if(parentTxn==null)
						parentTxn = Txn.ROOT_TRANSACTION;
				return beginChildTransaction(parentTxn, isolationLevel, dependent, parentTxn.isAdditive(), destinationTable);
		}

    private static final Logger LOG = Logger.getLogger(ClientTxnLifecycleManager.class);

		@Override
		public Txn beginChildTransaction(TxnView parentTxn,
																		 Txn.IsolationLevel isolationLevel,
																		 boolean isDependent,
																		 boolean additive,
																		 byte[] destinationTable) throws IOException {
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"Beginning child transaction: parent=%s,isolationLevel=%s,isDependent=%b,additive=%b,destinationTable=%s,isReadOnly=%b",parentTxn,isolationLevel,isDependent,additive,destinationTable,destinationTable==null);
				if(parentTxn==null)
						parentTxn = Txn.ROOT_TRANSACTION;
				if(destinationTable!=null && !parentTxn.allowsWrites())
						throw new DoNotRetryIOException("Cannot create a writable child of a read-only transaction. Elevate the parent transaction("+parentTxn.getTxnId()+") first");
        if(parentTxn.getState()!= Txn.State.ACTIVE)
            throw new DoNotRetryIOException("Cannot create a child of an inactive transaction. Parent: "+ parentTxn);
				if(destinationTable!=null){
						long timestamp = timestampSource.nextTimestamp();
						return createWritableTransaction(timestamp,isolationLevel,isDependent,additive,parentTxn,destinationTable);
				}else
						return createReadableTransaction(isolationLevel,isDependent,additive,parentTxn);
		}
		@Override
		public Txn chainTransaction(TxnView parentTxn,
																Txn.IsolationLevel isolationLevel,
																boolean dependent,
																boolean additive,
																byte[] destinationTable,
																Txn txnToCommit) throws IOException {
				if(parentTxn==null)
						parentTxn = Txn.ROOT_TRANSACTION;
        if(destinationTable!=null){
            /*
             * the new transaction must be writable, so we have to make sure that we generate a timestamp
             */
            if(!parentTxn.allowsWrites())
                throw new DoNotRetryIOException("Cannot create a writable child of a read-only transaction. Elevate the parent transaction("+parentTxn.getTxnId()+") first");
            if(!txnToCommit.allowsWrites())
                throw new DoNotRetryIOException("Cannot chain a writable transaction from a read-only transaction. Elevate the transaction("+txnToCommit.getTxnId()+") first");
        }

        if(!txnToCommit.allowsWrites() && Txn.ROOT_TRANSACTION.equals(parentTxn)){
            /*
             * The transaction to commit is read only, but we need to create a new parent transaction,
             * so we cannot chain transactions
             */
            throw new DoNotRetryIOException("Cannot chain a read-only parent transaction from a read-only transaction. Elevate the transaction("+txnToCommit.getTxnId()+") first");
        }
        txnToCommit.commit();
        long oldTs = txnToCommit.getCommitTimestamp();

				if(destinationTable!=null)
						return createWritableTransaction(oldTs,isolationLevel,dependent,additive,parentTxn,destinationTable);
				else{
						if(parentTxn.equals(Txn.ROOT_TRANSACTION)){
								return ReadOnlyTxn.createReadOnlyParentTransaction(oldTs,oldTs,isolationLevel,this,additive);
						}else{
								return ReadOnlyTxn.createReadOnlyChildTransaction(parentTxn,this,dependent,additive);
						}
				}
		}

		@Override
		public Txn elevateTransaction(Txn txn, byte[] destinationTable) throws IOException {
				if(!txn.allowsWrites()){
						//we've elevated from a read-only to a writable, so make sure that we add
						//it to the keep alive
						Txn writableTxn = new WritableTxn(txn,this,destinationTable);
						store.recordNewTransaction(writableTxn);
						keepAliveScheduler.scheduleKeepAlive(writableTxn);
						txn = writableTxn;
				}else
						store.elevateTransaction(txn,destinationTable);
				return txn;
		}

		@Override
		public long commit(long txnId) throws IOException {
				return store.commit(txnId);
				//TODO -sf- add the transaction to the global cache?
		}

		@Override
		public void rollback(long txnId) throws IOException {
				store.rollback(txnId);
				//TODO -sf- add the transaction to the global cache?
		}

		/**********************************************************************************************************/
		/*private helper method*/
		private Txn createWritableTransaction(long timestamp,
																					Txn.IsolationLevel isolationLevel,
																					boolean isDependent,
																					boolean additive,
																					TxnView parentTxn,
																					byte[] destinationTable) throws IOException {
				/*
				 * Create a writable transaction directly.
				 *
				 * This uses 2 network calls--once to get a beginTimestamp, and then once to record the
				 * transaction to the table.
				 */
				if(parentTxn!=null &&!Txn.ROOT_TRANSACTION.equals(parentTxn))
						parentTxn = new LazyTxn(parentTxn.getTxnId(),store,
										true,parentTxn.isDependent(),true,parentTxn.isAdditive(),parentTxn.getIsolationLevel()); //TODO -sf- should this be here?
				WritableTxn newTxn = new WritableTxn(timestamp,
								timestamp,isolationLevel,parentTxn,this,isDependent,additive,destinationTable);
				//record the transaction on the transaction table--network call
				store.recordNewTransaction(newTxn);
				keepAliveScheduler.scheduleKeepAlive(newTxn);

				return newTxn;
		}

		private Txn createReadableTransaction(Txn.IsolationLevel isolationLevel,
																					boolean isDependent,
																					boolean additive,
																					TxnView parentTxn) {
				/*
				 * Creates an elevatable, read-only transaction.
				 *
				 * This makes a network call if we are creating a new top-level transaction, otherwise, it
				 * will inherit timestamp and parent transaction information from its parent
				 *
				 * This comes in one of two forms:
				 * 1. top-level transaction(parentTxn ==Txn.ROOT_TRANSACTION or parentTxn == null)
				 * 2. child transaction (parentTxn!=null && parentTxn.getTxnId()>=0)
				 *
				 * In case 2, we don't even need to generate a new transaction id--we'll just inherit from
				 * the parent. However, we will need to generate a new transaction id UPON ELEVATION. We
				 * do this by providing a subclass of the ReadOnly transaction
				 *
				 */
				if(parentTxn.equals(Txn.ROOT_TRANSACTION)){
						long beginTimestamp = timestampSource.nextTimestamp();
						return ReadOnlyTxn.createReadOnlyParentTransaction(beginTimestamp,beginTimestamp,isolationLevel,this,additive);
				}else{
						return ReadOnlyTxn.createReadOnlyChildTransaction(parentTxn,this,isDependent,additive);
				}
		}

}
