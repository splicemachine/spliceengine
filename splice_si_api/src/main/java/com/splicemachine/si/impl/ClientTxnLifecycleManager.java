/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.si.impl;

import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.txn.*;
import com.splicemachine.si.impl.txn.ReadOnlyTxn;
import com.splicemachine.si.impl.txn.WritableTxn;
import com.splicemachine.timestamp.api.TimestampSource;

import java.io.IOException;

/**
 * Represents a Client Txn Lifecycle Manager.
 * <p/>
 * This class makes decisions like when a ReadOnly transaction is created instead of a writeable,
 * when a transaction is recorded to the transaction table, and so on.
 *
 * @author Scott Fines
 *         Date: 6/20/14
 */
@ThreadSafe
public class ClientTxnLifecycleManager implements TxnLifecycleManager{

    @ThreadSafe private final TimestampSource timestampSource;
    @ThreadSafe private TransactionStore store;
    @ThreadSafe private final ExceptionFactory exceptionFactory;

    private volatile boolean restoreMode=false;

    public ClientTxnLifecycleManager(TimestampSource timestampSource,
                                     ExceptionFactory exceptionFactory){
        this.timestampSource = timestampSource;
        this.exceptionFactory= exceptionFactory;
    }

    public void setTxnStore(TransactionStore store){
        this.store = store;
    }

    @Override
    public Txn beginTransaction() throws IOException{
        return beginTransaction(Txn.ROOT_TRANSACTION.getIsolationLevel());
    }

    @Override
    public Txn beginTransaction(byte[] destinationTable) throws IOException{
        return beginChildTransaction(Txn.ROOT_TRANSACTION,destinationTable);
    }

    @Override
    public Txn beginTransaction(Txn.IsolationLevel isolationLevel) throws IOException{
        return beginChildTransaction(Txn.ROOT_TRANSACTION,isolationLevel,null);
    }

    @Override
    public Txn beginTransaction(Txn.IsolationLevel isolationLevel,byte[] destinationTable) throws IOException{
        return beginChildTransaction(Txn.ROOT_TRANSACTION,isolationLevel,destinationTable);
    }

    @Override
    public Txn beginChildTransaction(TxnView parentTxn,byte[] destinationTable) throws IOException{
        if(parentTxn==null)
            parentTxn=Txn.ROOT_TRANSACTION;
        return beginChildTransaction(parentTxn,parentTxn.getIsolationLevel(),parentTxn.isAdditive(),destinationTable);
    }

    @Override
    public Txn beginChildTransaction(TxnView parentTxn,Txn.IsolationLevel isolationLevel,byte[] destinationTable) throws IOException{
        if(parentTxn==null)
            parentTxn=Txn.ROOT_TRANSACTION;
        return beginChildTransaction(parentTxn,isolationLevel,parentTxn.isAdditive(),destinationTable);
    }

    @Override
    public Txn beginChildTransaction(TxnView parentTxn,
                                     Txn.IsolationLevel isolationLevel,
                                     boolean additive,
                                     byte[] destinationTable) throws IOException{
        if(parentTxn==null)
            parentTxn=Txn.ROOT_TRANSACTION;
        if(destinationTable!=null && !parentTxn.allowsWrites())
            throw exceptionFactory.doNotRetry("Cannot create a writable child of a read-only transaction. Elevate the parent transaction("+parentTxn.getTxnId()+") first");
        if(parentTxn.getState()!=Txn.State.ACTIVE)
            throw exceptionFactory.doNotRetry("Cannot create a child of an inactive transaction. Parent: "+parentTxn);
        if(destinationTable!=null){
            long timestamp=timestampSource.nextTimestamp();
            return createWritableTransaction(timestamp,isolationLevel,additive,parentTxn,destinationTable);
        }else
            return createReadableTransaction(isolationLevel,additive,parentTxn);
    }

    @Override
    public Txn chainTransaction(TxnView parentTxn,
                                Txn.IsolationLevel isolationLevel,
                                boolean additive,
                                byte[] destinationTable,
                                Txn txnToCommit) throws IOException{
        if(parentTxn==null)
            parentTxn=Txn.ROOT_TRANSACTION;
        if(destinationTable!=null){
            /*
             * the new transaction must be writable, so we have to make sure that we generate a timestamp
             */
            if(!parentTxn.allowsWrites())
                throw exceptionFactory.doNotRetry("Cannot create a writable child of a read-only transaction. Elevate the parent transaction("+parentTxn.getTxnId()+") first");
            if(!txnToCommit.allowsWrites())
                throw exceptionFactory.doNotRetry("Cannot chain a writable transaction from a read-only transaction. Elevate the transaction("+txnToCommit.getTxnId()+") first");
        }

        if(!txnToCommit.allowsWrites() && Txn.ROOT_TRANSACTION.equals(parentTxn)){
            /*
             * The transaction to commit is read only, but we need to create a new parent transaction,
             * so we cannot chain transactions
             */
            throw exceptionFactory.doNotRetry("Cannot chain a read-only parent transaction from a read-only transaction. Elevate the transaction("+txnToCommit.getTxnId()+") first");
        }
        txnToCommit.commit();
        long oldTs=txnToCommit.getCommitTimestamp();

        if(destinationTable!=null)
            return createWritableTransaction(oldTs,isolationLevel,additive,parentTxn,destinationTable);
        else{
            if(parentTxn.equals(Txn.ROOT_TRANSACTION)){
                return ReadOnlyTxn.createReadOnlyParentTransaction(oldTs,oldTs,isolationLevel,this,exceptionFactory,additive);
            }else{
                return ReadOnlyTxn.createReadOnlyTransaction(oldTs,parentTxn,oldTs,isolationLevel,additive,this,exceptionFactory);
            }
        }
    }

    @Override
    public void enterRestoreMode(){
        this.restoreMode=true;
    }

    @Override
    public Txn elevateTransaction(Txn txn,byte[] destinationTable) throws IOException{
        if(!txn.allowsWrites()){
            //we've elevated from a read-only to a writable, so make sure that we add
            //it to the keep alive
            Txn writableTxn=new WritableTxn(txn,this,destinationTable,exceptionFactory);
            store.recordNewTransaction(writableTxn);
            keepAliveScheduler.scheduleKeepAlive(writableTxn);
            txn=writableTxn;
        }else
            store.elevateTransaction(txn,destinationTable);
        return txn;
    }

    @Override
    public long commit(long txnId) throws IOException{
        if(restoreMode){
            return -1; // we are in restore mode, don't try to access the store
        }
        return store.commit(txnId);
        //TODO -sf- add the transaction to the global cache?
    }

    @Override
    public void rollback(long txnId) throws IOException{
        if(restoreMode){
            return; // we are in restore mode, don't try to access the store
        }
        store.rollback(txnId);
        //TODO -sf- add the transaction to the global cache?
    }

    /**********************************************************************************************************/
        /*private helper method*/
    private Txn createWritableTransaction(long timestamp,
                                          Txn.IsolationLevel isolationLevel,
                                          boolean additive,
                                          TxnView parentTxn,
                                          byte[] destinationTable) throws IOException{
		/*
		 * Create a writable transaction directly.
		 *
		 * This uses 2 network calls--once to get a beginTimestamp, and then once to record the
		 * transaction to the table.
		 */
        WritableTxn newTxn=new WritableTxn(timestamp,
                timestamp,isolationLevel,parentTxn,this,additive,destinationTable,exceptionFactory);
        //record the transaction on the transaction table--network call
        store.recordNewTransaction(newTxn);
        keepAliveScheduler.scheduleKeepAlive(newTxn);

        return newTxn;
    }

    private Txn createReadableTransaction(Txn.IsolationLevel isolationLevel,
                                          boolean additive,
                                          TxnView parentTxn){
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
            long beginTimestamp=timestampSource.nextTimestamp();
            return ReadOnlyTxn.createReadOnlyParentTransaction(beginTimestamp,beginTimestamp,isolationLevel,this,exceptionFactory,additive);
        }else{
            return ReadOnlyTxn.createReadOnlyChildTransaction(parentTxn,this,additive,exceptionFactory);
        }
    }

}
