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

package com.splicemachine.si.impl.store;

import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.api.txn.*;
import com.splicemachine.timestamp.api.TimestampSource;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Simple Txn store useful for Unit testing. NOT TO BE USED OUTSIDE OF TESTING. This is not thread safe!
 *
 * @author Scott Fines
 *         Date: 6/23/14
 */
public class TestingTxnStore implements TransactionStore {
    private final Map<Long, Txn> txnMap;
    private final TimestampSource logicalTimestampSource;
    private final TimestampSource physicalTimestampSource;

    private final Clock clock;
    private TxnLifecycleManager tc;
    private final ExceptionFactory exceptionFactory;
    private long lookupCount = 0;
    private long elevationCount = 0;
    private long createdCount = 0;
    private long rollbackCount = 0;
    private long commitCount = 0;

    public TestingTxnStore(Clock clock,
                           TimestampSource logicalTimestampSource,
                           TimestampSource physicalTimestampSource,
                           ExceptionFactory exceptionFactory,
                           TxnSupplier GlobalTxnCacheSupplier){
        this.txnMap=new ConcurrentHashMap<>();
        this.logicalTimestampSource = logicalTimestampSource;
        this.physicalTimestampSource = physicalTimestampSource;
        this.exceptionFactory=exceptionFactory;
        this.clock=clock;
    }

    @Override
    public Txn getTransaction(long txnId) throws IOException{
        lookupCount++;
        return txnMap.get(txnId);
    }

    @Override
    public Txn[] getTransactions(long[] txnIds) throws IOException{
        Txn[] txns = new Txn[txnIds.length];
        for (int i = 0; i< txnIds.length; i++ ) {
            txns[i] = getTransaction(txnIds[i]);
        }
        return txns;
    }

    @Override
    public void cache(Txn toCache){
        txnMap.put(toCache.getTxnId(),toCache);
    }

    @Override
    public void cache(Txn[] toCache){
        for (int i =0; i< toCache.length;i++)
            cache(toCache[i]);
    }

    @Override
    public void recordNewTransaction(Txn txn) throws IOException{
        assert txnMap.get(txn.getTxnId())==null:" Txn "+txn.getTxnId()+" already existed!";
        createdCount++;
        txnMap.put(txn.getTxnId(),txn);
    }

    @Override
    public Txn rollback(Txn rollbackTxn) throws IOException{
        rollbackCount++;
        txnMap.put(rollbackTxn.getTxnId(),rollbackTxn);
        return rollbackTxn;
    }

    @Override
    public Txn[] rollback(Txn[] transactions) throws IOException {
        for (int i = 0; i< transactions.length; i++)
            transactions[i] = rollback(transactions[i]);
        return transactions;
    }

    @Override
    public Txn commit(Txn txn) throws IOException{
        commitCount++;
        Txn activeTxn=txnMap.get(txn.getTxnId());
        if(activeTxn == null || !activeTxn.isAbleToCommit())
            throw new IOException("Cannot commit txn txn: " + txn);
        txn.setCommitTimestamp(logicalTimestampSource.nextTimestamp());
        txn.setHLCTimestamp(physicalTimestampSource.nextTimestamp());
        txn.persist();
        txnMap.put(txn.getTxnId(),txn);
        return txn;
    }

    @Override
    public Txn[] commit(Txn[] transactions) throws IOException {
        for (int i = 0; i< transactions.length; i++)
            transactions[i] = commit(transactions[i]);
        return transactions;
    }

    @Override
    public void elevateTransaction(Txn txn) throws IOException{
        elevationCount++;
        txnMap.put(txn.getTxnId(),txn);
    }

    @Override
    public void elevateTransaction(Txn[] transactions) throws IOException {
        for (int i = 0; i< transactions.length; i++)
            elevateTransaction(transactions[i]);
    }

    @Override
    public long lookupCount(){
        return lookupCount;
    }

    @Override
    public long elevationCount(){
        return elevationCount;
    }

    @Override
    public long createdCount(){
        return createdCount;
    }

    @Override
    public long rollbackCount(){
        return rollbackCount;
    }

    @Override
    public long commitCount(){
        return commitCount;
    }

}
