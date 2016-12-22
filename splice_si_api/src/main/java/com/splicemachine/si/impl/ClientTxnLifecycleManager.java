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
public class ClientTxnLifecycleManager implements TxnLifecycleManager {

    @ThreadSafe private final TimestampSource logicalTimestampSource;
    @ThreadSafe private final TimestampSource physicalTimestampSource;
    @ThreadSafe private TransactionStore store;
    @ThreadSafe private final ExceptionFactory exceptionFactory;
    @ThreadSafe private final TxnFactory txnFactory;
    @ThreadSafe private final TxnLocationFactory txnLocationFactory;
    @ThreadSafe private final TxnSupplier globalTxnCache;
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
        txn.setHLCTimestamp(physicalTimestampSource.nextTimestamp());
        txn.setTxnId(logicalTimestampSource.nextTimestamp());
        txn.setRegionId(txnLocationFactory.getRegionId());
        txn.setNodeId(txnLocationFactory.getNodeId());
        txn.setCommitTimestamp(-2L); // Active
        return txn;
    }


    @Override
    public Txn beginChildTransaction(Txn parentTxn) throws IOException{
        return createChildTransaction(parentTxn,null);
    }

    public Txn createChildTransaction(Txn parentTxn, Txn txnToCommit) throws IOException {
        assert parentTxn !=null:"Parent Txn cannot be null";
        Txn txn = txnFactory.getTxn();
        txn.setHLCTimestamp(physicalTimestampSource.nextTimestamp());
        txn.setTxnId(txnToCommit !=null?store.commit(txnToCommit):logicalTimestampSource.nextTimestamp());
        txn.setRegionId(txnLocationFactory.getRegionId());
        txn.setNodeId(txnLocationFactory.getNodeId());
        txn.setCommitTimestamp(-2L); // Active
        txn.setParentTxnId(parentTxn.getTxnId());
        return txn;
    }

    @Override
    public Txn chainTransaction(Txn parentTxn,
                                Txn txnToCommit) throws IOException{
        assert txnToCommit!=null:"txnToCommit cannot be null";
        return createChildTransaction(parentTxn,txnToCommit);
    }

    @Override
    public void enterRestoreMode(){
        this.restoreMode=true;
    }

    @Override
    public Txn elevateTransaction(Txn txn) throws IOException{
        assert txn!=null:"txn cannot be null";
        store.elevateTransaction(txn);
        return txn;
    }

    @Override
    public long commit(Txn txn) throws IOException{
        assert txn!=null:"txn cannot be null";
        if(restoreMode){
            return -1; // we are in restore mode, don't try to access the store
        }
        long commitTs = store.commit(txn);
        globalTxnCache.cache(txn);
        return commitTs;
    }

    @Override
    public void rollback(Txn txn) throws IOException{
        assert txn!=null:"txn cannot be null";
        if(restoreMode){
            return; // we are in restore mode, don't try to access the store
        }
        store.rollback(txn);
        globalTxnCache.cache(txn);
    }

}
