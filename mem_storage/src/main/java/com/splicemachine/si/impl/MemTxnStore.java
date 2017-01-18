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

import com.splicemachine.concurrent.LongStripedSynchronizer;
import com.splicemachine.si.api.txn.*;

import javax.ws.rs.NotSupportedException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * In-memory representation of a full Transaction Store.
 * <p/>
 * This is useful primarily for testing--that way, we don't need an HBase cluster running to test most of our
 * logic.
 *
 * @author Scott Fines
 *         Date: 6/23/14
 */
public class MemTxnStore implements TransactionStore{
    private LongStripedSynchronizer<ReadWriteLock> lockStriper;
    private final ConcurrentMap<Long, Txn> txnMap;

    public MemTxnStore(){
        this.txnMap=new ConcurrentHashMap<>();
    }

    @Override
    public Txn getTransaction(long txnId) throws IOException{
        ReadWriteLock rwlLock=lockStriper.get(txnId);
        Lock rl=rwlLock.readLock();
        rl.lock();
        try{
            return txnMap.get(txnId);
        }finally{
            rl.unlock();
        }
    }

    @Override
    public void recordNewTransaction(Txn txn) throws IOException{
        ReadWriteLock readWriteLock=lockStriper.get(txn.getTxnId());
        Lock wl=readWriteLock.writeLock();
        wl.lock();
        try{
            Txn txn1=txnMap.get(txn.getTxnId());
            assert txn1==null:" Transaction "+txn.getTxnId()+" already existed!";
            txnMap.put(txn.getTxnId(),txn);
        }finally{
            wl.unlock();
        }
    }

    @Override
    public Txn rollback(Txn txn) throws IOException{
        long txnId = txn.getTxnId();
        ReadWriteLock readWriteLock=lockStriper.get(txnId);
        Lock wl=readWriteLock.writeLock();
        wl.lock();
        try{
            txnMap.put(txnId,txn);
            return txn;
        }finally{
            wl.unlock();
        }
    }

    @Override
    public Txn commit(Txn txn) throws IOException{
        long txnId = txn.getTxnId();
        ReadWriteLock readWriteLock=lockStriper.get(txnId);
        Lock wl=readWriteLock.writeLock();
        wl.lock();
        try {
            txnMap.put(txnId,txn);
            return txn;
        } finally{
            wl.unlock();
        }
    }

    @Override
    public void elevateTransaction(Txn txn) throws IOException{
        long txnId=txn.getTxnId();
        ReadWriteLock readWriteLock=lockStriper.get(txnId);
        Lock wl=readWriteLock.writeLock();
        wl.lock();
        try{
            txnMap.put(txnId,txn);
        }finally{
            wl.unlock();
        }
    }

    @Override
    public long lookupCount(){
        return 0;
    }

    @Override
    public long elevationCount(){
        return 0;
    }

    @Override
    public long createdCount(){
        return 0;
    }

    @Override
    public long rollbackCount(){
        return 0;
    }

    @Override
    public long commitCount(){
        return 0;
    }

    @Override
    public Txn[] rollback(Txn[] transaction) throws IOException {
        assert transaction != null:"Transaction Array Passed in is null";
        Txn[] txns = new Txn[transaction.length];
        for (int i=0; i<transaction.length; i++) {
            txns[i] = rollback(transaction[i]);
        }
        return txns;
    }

    @Override
    public Txn[] commit(Txn[] transaction) throws IOException {
        assert transaction != null:"Transaction Array Passed in is null";
        Txn[] txns = new Txn[transaction.length];
        for (int i=0; i<transaction.length; i++) {
            txns[i] = commit(transaction[i]);
        }
        return txns;
    }

    @Override
    public void elevateTransaction(Txn[] transaction) throws IOException {
        assert transaction != null:"Transaction Array Passed in is null";
        for (int i=0; i<transaction.length; i++) {
            elevateTransaction(transaction[i]);
        }
    }

    @Override
    public Txn[] getTransactions(long[] txnIds) throws IOException {
        assert txnIds != null:"Transaction Array Passed in is null";
        Txn[] txns = new Txn[txnIds.length];
        for (int i=0; i<txnIds.length; i++) {
            txns[i] = getTransaction(txnIds[i]);
        }
        return txns;
    }

    @Override
    public void cache(Txn toCache) {
        throw new NotSupportedException("not implemented");
    }

    @Override
    public void cache(Txn[] toCache) {
        assert toCache != null:"Transaction Array Passed in is null";
        Txn[] txns = new Txn[toCache.length];
        for (int i=0; i<toCache.length; i++) {
            cache(toCache[i]);
        }
    }
}