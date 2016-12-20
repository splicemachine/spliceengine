package com.splicemachine.si.impl.store;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.splicemachine.si.api.txn.Transaction;
import com.splicemachine.si.api.txn.TxnSupplier;

import java.io.IOException;

/**
 *
 *
 *
 */
public class ThreadTxnCacheSupplier implements TxnSupplier {
    private ConcurrentLinkedHashMap<Long, Transaction> cache; // autobox for now

    public ThreadTxnCacheSupplier(long maxSize, int concurrencyLevel) {
        cache=new ConcurrentLinkedHashMap.Builder<Long, Transaction>()
                .maximumWeightedCapacity(maxSize)
                .concurrencyLevel(concurrencyLevel)
                .build();
    }


    @Override
    public Transaction getTransaction(long txnId) throws IOException {
        return cache.get(txnId);
    }

    @Override
    public void cache(Transaction toCache) {
        cache.put(toCache.getTransactionId(),toCache);
    }
}
