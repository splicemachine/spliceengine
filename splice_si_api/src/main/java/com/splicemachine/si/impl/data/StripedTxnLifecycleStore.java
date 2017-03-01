/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.si.impl.data;

import com.splicemachine.concurrent.LongStripedSynchronizer;
import com.splicemachine.si.api.txn.lifecycle.TxnLifecycleStore;
import com.splicemachine.access.api.ServerControl;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.lifecycle.TxnPartition;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.timestamp.api.TimestampSource;
import com.splicemachine.utils.Source;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * @author Scott Fines
 *         Date: 12/14/15
 */
public class StripedTxnLifecycleStore implements TxnLifecycleStore{

    private static final Logger LOG=Logger.getLogger(StripedTxnLifecycleStore.class);

    private static final TxnMessage.Txn NONEXISTENT_TXN;

    static{
        TxnMessage.TxnInfo nonExistentInfo=TxnMessage.TxnInfo.newBuilder()
                .setBeginTs(-Long.MAX_VALUE)
                .setTxnId(-Long.MAX_VALUE)
                .setIsolationLevel(Txn.IsolationLevel.SNAPSHOT_ISOLATION.getLevel()).build();

        NONEXISTENT_TXN=TxnMessage.Txn.newBuilder().setState(Txn.State.ROLLEDBACK.getId()).setInfo(nonExistentInfo).build();
    }

    private final LongStripedSynchronizer<ReadWriteLock> lockStriper;
    private final TxnPartition baseStore;
    private final ServerControl serverControl;
    private final TimestampSource timestampSource;

    public StripedTxnLifecycleStore(int numPartitions,
                                    TxnPartition baseStore,
                                    ServerControl serverControl,TimestampSource timestampSource){
        this.lockStriper=LongStripedSynchronizer.stripedReadWriteLock(numPartitions,false);
        this.baseStore=baseStore;
        this.serverControl=serverControl;
        this.timestampSource=timestampSource;
    }

    @Override
    public void beginTransaction(TxnMessage.TxnInfo txn) throws IOException{
        Lock lock=lockStriper.get(txn.getTxnId()).writeLock();
        acquireLock(lock);
        try{
            baseStore.recordTransaction(txn);
        }finally{
            unlock(lock);
        }
    }


    @Override
    public void elevateTransaction(long txnId,byte[] destTable) throws IOException{
        Lock lock=lockStriper.get(txnId).writeLock();
        acquireLock(lock);
        try{
            baseStore.addDestinationTable(txnId,destTable);
        }finally{
            unlock(lock);
        }
    }

    @Override
    public long commitTransaction(long txnId) throws IOException{
        Lock lock=lockStriper.get(txnId).writeLock();
        acquireLock(lock);
        try{
            Txn.State state=baseStore.getState(txnId);
            if(state==null){
//                LOG.warn("Attempting to commit a read-only transaction. Waste of a network call");
                return -1l; //no need to acquire a new timestamp if we have a read-only transaction
            }
            if(state==Txn.State.COMMITTED){
                SpliceLogUtils.warn(LOG,"attempting to commit already committed txn=%d",txnId);
                return baseStore.getCommitTimestamp(txnId);
            }
            if(state==Txn.State.ROLLEDBACK) {
                SpliceLogUtils.error(LOG,"attempting to commit rolled back txn=%d",txnId);
                throw baseStore.cannotCommit(txnId, state);
            }
            long commitTs=timestampSource.nextTimestamp();
            baseStore.recordCommit(txnId,commitTs);
            return commitTs;
        }finally{
            unlock(lock);
        }
    }

    @Override
    public void rollbackTransaction(long txnId) throws IOException{
        Lock lock=lockStriper.get(txnId).writeLock();
        acquireLock(lock);
        try{
            Txn.State state=baseStore.getState(txnId);
            if(state==null){
                return;
            }
            switch(state){
                case COMMITTED:
                    return;
                case ROLLEDBACK:
                    return;
                default:
                    baseStore.recordRollback(txnId);
            }
        }finally{
            unlock(lock);
        }
    }

    @Override
    public void rollbackSubtransactions(long txnId, long[] subIds) throws IOException {
        long beginTS = txnId & SIConstants.TRANSANCTION_ID_MASK;

        Lock lock=lockStriper.get(beginTS).writeLock();
        acquireLock(lock);
        try{
            Txn.State state=baseStore.getState(beginTS);
            if(state==null){
                return;
            }
            switch(state){
                case COMMITTED:
                    return;
                case ROLLEDBACK:
                    return;
                default:
                    baseStore.recordRollbackSubtransactions(txnId, subIds);
            }
        }finally{
            unlock(lock);
        }
    }

    @Override
    public boolean keepAlive(long txnId) throws IOException{
        Lock lock=lockStriper.get(txnId).writeLock();
        acquireLock(lock);
        try{
            return baseStore.keepAlive(txnId);
        }finally{
            unlock(lock);
        }
    }


    @Override
    public TxnMessage.Txn getTransaction(long txnId) throws IOException{
        long beginTS = txnId & SIConstants.TRANSANCTION_ID_MASK;
        Lock lock=lockStriper.get(beginTS).readLock();
        acquireLock(lock);
        try{
            TxnMessage.Txn txn=baseStore.getTransaction(txnId);
            if(txn==null)
                txn=NONEXISTENT_TXN;
            return txn;
        }finally{
            unlock(lock);
        }
    }

    @Override
    public long[] getActiveTransactionIds(byte[] destTable,long startId,long endId) throws IOException{
        if(endId<0)
            endId = Long.MAX_VALUE;
        return baseStore.getActiveTxnIds(startId,endId,destTable);
    }

    @Override
    public Source<TxnMessage.Txn> getActiveTransactions(byte[] destTable,long startId,long endId) throws IOException{
        if(endId<0)
            endId = Long.MAX_VALUE;
        return baseStore.getActiveTxns(startId,endId,destTable);
    }

    @Override
    public void rollbackTransactionsAfter(long txnId) throws IOException {
        baseStore.rollbackTransactionsAfter(txnId);
    }
    /* ***************************************************************************************************************/
    /*private helper methods*/
    private void unlock(Lock lock) throws IOException{
        lock.unlock();
        serverControl.stopOperation();
    }

    private void acquireLock(Lock lock) throws IOException{
        //make sure that the region doesn't close while we are working on it

        serverControl.startOperation();
        boolean shouldContinue=true;
        while(shouldContinue){
            try{
                shouldContinue=!lock.tryLock(200,TimeUnit.MILLISECONDS);
                try{
                    /*
                     * Checks if the client has disconnected while acquiring this lock.
				     * If it has, we need to ensure that our lock is released (if it has been
	 			     * acquired).
    				 */
                    serverControl.ensureNetworkOpen();
                }catch(IOException ioe){
                    if(!shouldContinue) //the lock was acquired, so it needs to be unlocked
                        unlock(lock);
                    throw ioe;
                }
            }catch(InterruptedException e){
                throw new IOException(e);
            }
        }
    }
}
