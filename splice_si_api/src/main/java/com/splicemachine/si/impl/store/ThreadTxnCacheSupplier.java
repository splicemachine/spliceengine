package com.splicemachine.si.impl.store;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;

import java.io.IOException;

/**
 *
 *
 *
 */
public class ThreadTxnCacheSupplier implements TxnSupplier {
    private ConcurrentLinkedHashMap<Long, Txn> cache; // autobox for now

    public ThreadTxnCacheSupplier(long maxSize, int concurrencyLevel) {
        cache=new ConcurrentLinkedHashMap.Builder<Long, Txn>()
                .maximumWeightedCapacity(maxSize)
                .concurrencyLevel(concurrencyLevel)
                .build();
    }


    @Override
    public Txn getTransaction(long txnId) throws IOException {
        return cache.get(txnId);
    }

    @Override
    public void cache(Txn toCache) {
        cache.put(toCache.getTxnId(),toCache);
    }
}
