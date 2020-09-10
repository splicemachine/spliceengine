/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.si.impl;

import com.carrotsearch.hppc.LongHashSet;
import com.splicemachine.annotations.ThreadSafe;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.txn.*;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.txn.ReadOnlyTxn;
import com.splicemachine.si.impl.txn.WritableTxn;
import com.splicemachine.timestamp.api.TimestampSource;

import java.io.IOException;
import java.nio.charset.Charset;

/**
 * Represents a Client Transaction Lifecycle Manager.
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
    @ThreadSafe private TxnStore store;
    @ThreadSafe private KeepAliveScheduler keepAliveScheduler;
    @ThreadSafe private final ExceptionFactory exceptionFactory;

    private volatile boolean restoreMode=false;

    private volatile String replicationRole = SIConstants.REPLICATION_ROLE_NONE;

    public ClientTxnLifecycleManager(TimestampSource timestampSource,
                                     ExceptionFactory exceptionFactory){
        this.timestampSource = timestampSource;
        this.exceptionFactory= exceptionFactory;
    }

    public void setTxnStore(TxnStore store){
        this.store = store;
    }

    public void setKeepAliveScheduler(KeepAliveScheduler kas){
        this.keepAliveScheduler = kas;
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
                                     byte[] destinationTable) throws IOException {
        return beginChildTransaction(parentTxn, isolationLevel, additive, destinationTable, false);
    }

    @Override
    public Txn beginChildTransaction(TxnView parentTxn,
                                     Txn.IsolationLevel isolationLevel,
                                     boolean additive,
                                     byte[] destinationTable,
                                     boolean inMemory) throws IOException{
        return beginChildTransaction(parentTxn, isolationLevel, additive, destinationTable, inMemory, null);
    }

    @Override
    public Txn beginChildTransaction(TxnView parentTxn,
                                     Txn.IsolationLevel isolationLevel,
                                     boolean additive,
                                     byte[] destinationTable,
                                     boolean inMemory,
                                     TaskId taskId) throws IOException {

        if(parentTxn==null)
            parentTxn=Txn.ROOT_TRANSACTION;
        if(destinationTable!=null && !parentTxn.allowsWrites())
            throw exceptionFactory.doNotRetry("Cannot create a writable child of a read-only transaction. Elevate the parent transaction("+parentTxn.getTxnId()+") first");
        if(parentTxn.getState()!=Txn.State.ACTIVE)
            throw exceptionFactory.doNotRetry("Cannot create a child of an inactive transaction. Parent: "+parentTxn);
        if(destinationTable!=null){
            if (inMemory && parentTxn.allowsSubtransactions()) {
                Txn parent = (Txn) parentTxn;
                long subId = parent.newSubId();
                if (subId <= SIConstants.SUBTRANSANCTION_ID_MASK)
                    return createWritableTransaction(parent.getBeginTimestamp(), subId, parent, isolationLevel, additive, parentTxn, destinationTable, null);
            }
            long timestamp = getTimestamp();
            return createWritableTransaction(timestamp, 0, null, isolationLevel, additive, parentTxn, destinationTable, taskId);
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
            return createWritableTransaction(oldTs, 0, null, isolationLevel,additive,parentTxn,destinationTable, null);
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
    public boolean isRestoreMode() {
        return this.restoreMode;
    }

    @Override
    public void setReplicationRole (String role) {
        this.replicationRole = role;
    }

    @Override
    public String getReplicationRole() {
        return replicationRole;
    }

    @Override
    public Txn elevateTransaction(Txn txn,byte[] destinationTable) throws IOException{
        if (replicationRole.compareToIgnoreCase(SIConstants.REPLICATION_ROLE_REPLICA) == 0 &&
                Bytes.compareTo(destinationTable, "replication".getBytes(Charset.defaultCharset().name())) != 0) {
            throw new IOException(StandardException.newException(SQLState.READ_ONLY));
        }
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

    @Override
    public void unregisterActiveTransaction(long txnId) throws IOException{
        store.unregisterActiveTransaction(txnId);
    }

    @Override
    public void rollbackSubtransactions(long txnId, LongHashSet rolledback) throws IOException {
        if(restoreMode){
            return; // we are in restore mode, don't try to access the store
        }
        store.rollbackSubtransactions(txnId, rolledback);
    }

    /**********************************************************************************************************/
        /*private helper method*/
    private Txn createWritableTransaction(long timestamp,
                                          long subId,
                                          Txn parentReference,
                                          Txn.IsolationLevel isolationLevel,
                                          boolean additive,
                                          TxnView parentTxn,
                                          byte[] destinationTable,
                                          TaskId taskId) throws IOException{
		if (restoreMode || replicationRole.compareToIgnoreCase(SIConstants.REPLICATION_ROLE_REPLICA) == 0 &&
        Bytes.compareTo(destinationTable, "replication".getBytes(Charset.defaultCharset().name())) != 0) {
            throw new IOException(StandardException.newException(SQLState.READ_ONLY));
        }
        /*
		 * Create a writable transaction directly.
		 *
		 * This uses 2 network calls--once to get a beginTimestamp, and then once to record the
		 * transaction to the table.
		 */
        WritableTxn newTxn=new WritableTxn(timestamp ^ subId, timestamp, parentReference,
                isolationLevel,parentTxn,this,additive,destinationTable,taskId,exceptionFactory);
        if (subId == 0) {
            //record the transaction on the transaction table--network call
            store.recordNewTransaction(newTxn);
            if (parentTxn == Txn.ROOT_TRANSACTION) {
                // keep track of this transaction
                store.registerActiveTransaction(newTxn);
            }
            keepAliveScheduler.scheduleKeepAlive(newTxn);
        }

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
            long beginTimestamp=getTimestamp();
            Txn newTxn = ReadOnlyTxn.createReadOnlyParentTransaction(beginTimestamp, beginTimestamp, isolationLevel, this, exceptionFactory, additive);
            // keep track of this transaction
            store.registerActiveTransaction(newTxn);
            return newTxn;
        }else{
            return ReadOnlyTxn.createReadOnlyChildTransaction(parentTxn,this,additive,exceptionFactory);
        }
    }

    private long getTimestamp() {
        if (isRestoreMode())
            return timestampSource.currentTimestamp();
        else if (replicationRole.compareToIgnoreCase(SIConstants.REPLICATION_ROLE_REPLICA) == 0)
            return timestampSource.currentTimestamp();
        else
            return timestampSource.nextTimestamp();
    }
}
