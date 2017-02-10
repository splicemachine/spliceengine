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

package com.splicemachine.si.impl.store;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TxnSupplier which caches transaction which have "Completed"--i.e. which have entered the COMMITTED or ROLLEDBACK
 * state.
 * <p/>
 * This class is thread-safe, and safe to be shared between many threads.
 *
 * @author Scott Fines
 *         Date: 6/18/14
 */
public class CompletedTxnCacheSupplier implements TxnSupplier{
    private ConcurrentLinkedHashMap<Long, TxnView> cache; // autobox for now
    private final TxnSupplier delegate;
    private final AtomicLong hits=new AtomicLong();
    private final AtomicLong requests=new AtomicLong();

    public CompletedTxnCacheSupplier(TxnSupplier delegate,int maxSize,int concurrencyLevel){
        cache=new ConcurrentLinkedHashMap.Builder<Long, TxnView>()
                .maximumWeightedCapacity(maxSize)
                .concurrencyLevel(concurrencyLevel)
                .build();
        this.delegate=delegate;
    }

    public int getMaxSize(){
        return cache.size();
    }

    @Override
    public TxnView getTransaction(long txnId) throws IOException{
        if(txnId==-1)
            return Txn.ROOT_TRANSACTION;
        return getTransaction(txnId,false);
    }

    @Override
    @SuppressFBWarnings("SF_SWITCH_NO_DEFAULT") //intentional
    public TxnView getTransaction(long txnId,boolean getDestinationTables) throws IOException{
        if(txnId==-1)
            return Txn.ROOT_TRANSACTION;
        requests.incrementAndGet();
        TxnView txn=cache.get(txnId); // autobox until Cliff C merge
        if(txn!=null){
            hits.incrementAndGet();
            return txn;
        }
        //bummer, we aren't in the cache, need to check the delegate
        TxnView transaction=delegate.getTransaction(txnId,getDestinationTables);
        if(transaction==null) //noinspection ConstantConditions
            return transaction; //don't cache read-only transactions;

        switch(transaction.getEffectiveState()){
            case COMMITTED:
            case ROLLEDBACK:
                cache.put(transaction.getTxnId(),transaction); // Cache for Future Use
        }
        return transaction;
    }

    @Override
    public boolean transactionCached(long txnId){
        return cache.get(txnId)!=null;
    }

    @Override
    public void cache(TxnView toCache){
        if(toCache.getState()==Txn.State.ACTIVE) return; //cannot cache incomplete transactions
        cache.put(toCache.getTxnId(),toCache);
    }

    @Override
    public TxnView getTransactionFromCache(long txnId){
        requests.incrementAndGet();
        TxnView txn=cache.get(txnId);
        if(txn!=null)
            hits.incrementAndGet();
        return txn;
    }
}
