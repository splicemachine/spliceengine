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

import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.txn.*;
import com.splicemachine.timestamp.api.TimestampSource;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;
import java.util.Arrays;

/**
 * Represents a Client Txn Lifecycle Manager.
 * <p/>
 *
 * @author Scott Fines
 *         Date: 6/20/14
 */
@ThreadSafe
public class ClientTxnLifecycleManager implements TxnLifecycleManager {

    private final TimestampSource logicalTimestampSource;
    private final TimestampSource physicalTimestampSource;
    private TransactionStore store;
    private final ExceptionFactory exceptionFactory;
    private final TxnFactory txnFactory;
    private final TxnLocationFactory txnLocationFactory;
    private final TxnSupplier globalTxnCache;
    private volatile boolean restoreMode=false;

    public ClientTxnLifecycleManager(TimestampSource logicalTimestampSource,
                                     TimestampSource physicalTimestampSource,
                                     ExceptionFactory exceptionFactory,
                                     TxnFactory txnFactory,
                                     TxnLocationFactory txnLocationFactory,
                                     TxnSupplier globalTxnCache,
                                     TransactionStore transactionStore){
        assert logicalTimestampSource !=null:"logicalTimestampSource cannot be null";
        assert physicalTimestampSource !=null:"physicalTimestampSource cannot be null";
        assert exceptionFactory !=null:"exceptionFactory cannot be null";
        assert txnFactory !=null:"txnFactory cannot be null";
        assert txnLocationFactory !=null:"txnLocationFactory cannot be null";
        assert transactionStore !=null:"transactionStore cannot be null";
        this.logicalTimestampSource = logicalTimestampSource;
        this.physicalTimestampSource = physicalTimestampSource;
        this.exceptionFactory= exceptionFactory;
        this.txnFactory = txnFactory;
        this.txnLocationFactory = txnLocationFactory;
        this.globalTxnCache = globalTxnCache;
        this.store = transactionStore;
    }

    @Override
    public Txn beginTransaction() throws IOException{
        Txn txn = txnFactory.getTxn();
        txn.setTxnId(logicalTimestampSource.nextTimestamp());
        txn.setRegionId(txnLocationFactory.getRegionId());
        txn.setNodeId(txnLocationFactory.getNodeId());
        txn.setCommitTimestamp(Txn.ACTIVE); // Active
        return txn;
    }

    @Override
    public Txn[] beginTransaction(int batch) throws IOException {
        Txn[] txns = txnFactory.getTxn(batch);
        long[] logTimes = logicalTimestampSource.nextTimestamps(batch);
        for (int i =0; i< batch;i++) {
            txns[i].setTxnId(logTimes[i]);
            txns[i].setRegionId(txnLocationFactory.getRegionId());
            txns[i].setNodeId(txnLocationFactory.getNodeId());
            txns[i].setCommitTimestamp(Txn.ACTIVE);
        }
        return txns;
    }

    @Override
    public Txn beginChildTransaction(Txn parentTxn) throws IOException{
        return createChildTransaction(parentTxn,logicalTimestampSource.nextTimestamp());
    }

    @Override
    public Txn chainTransaction(Txn parentTxn,
                                Txn txnToCommit) throws IOException{
        assert txnToCommit!=null:"txnToCommit cannot be null";
        txnToCommit = store.commit(txnToCommit);
        return createChildTransaction(parentTxn,txnToCommit.getTxnId());
    }

    public Txn createChildTransaction(Txn parentTxn,long logicalTs) throws IOException {
        assert parentTxn !=null:"Parent Txn cannot be null";
        Txn txn = txnFactory.getTxn();
        txn.setTxnId(logicalTs);
        txn.setRegionId(txnLocationFactory.getRegionId());
        txn.setNodeId(txnLocationFactory.getNodeId());
        txn.setCommitTimestamp(Txn.ACTIVE); // Active
        txn.setParentTxnId(parentTxn.getTxnId());
        return txn;
    }

    @Override
    public Txn[] beginChildTransactions(Txn parentTxn, int batch) throws IOException {
        long[] logicalTimestamps = logicalTimestampSource.nextTimestamps(batch);
        Txn[] txns = new Txn[batch];
        for (int i = 0; i< batch; i++)
            txns[i] = createChildTransaction(parentTxn,logicalTimestamps[i]);
        return txns;
    }

    @Override
    public Txn[] elevateTransaction(Txn[] txn) throws IOException {
        assert txn!=null:"elevateTransaction txn[] cannot be null";
        for (int i = 0; i<txn.length;i++)
            txn[i] = elevateTransaction(txn[i]);
        return txn;
    }

    @Override
    public void enterRestoreMode(){
        this.restoreMode=true;
    }

    @Override
    public Txn elevateTransaction(Txn txn) throws IOException{
        assert txn!=null:"txn cannot be null";
        System.out.println("elevate: "+txn);
        System.out.println("store: "+store);
        store.elevateTransaction(txn);
        System.out.println("returnedTxn: "+txn);
        return txn;
    }

    @Override
    public Txn commit(Txn txn) throws IOException{
        assert txn!=null:"txn cannot be null";
        if(restoreMode){
            return null; // we are in restore mode, don't try to access the store
        }
        txn = store.commit(txn);
        globalTxnCache.cache(txn);
        return txn;
    }

    @Override
    public Txn[] commit(Txn[] txn) throws IOException{
        assert txn!=null:"txn cannot be null";
        if(restoreMode){
            Txn[] ts = new Txn[txn.length];
                    Arrays.fill(ts, null);
            return ts; // we are in restore mode, don't try to access the store
        }
        txn = store.commit(txn);
        globalTxnCache.cache(txn);
        return txn;
    }


    @Override
    public Txn rollback(Txn txn) throws IOException{
        assert txn!=null:"txn cannot be null";
        if(restoreMode){
            return null; // we are in restore mode, don't try to access the store
        }
        txn = store.rollback(txn);
        globalTxnCache.cache(txn);
        return txn;
    }

}
