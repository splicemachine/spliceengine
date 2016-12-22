package com.splicemachine.si.impl.store;

import com.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import java.io.IOException;

/**
 *
 *
 */
public class GlobalTxnCacheSupplier implements TxnSupplier {
    private ConcurrentLinkedHashMap<Long, Txn> cache; // autobox for now

    public GlobalTxnCacheSupplier(long maxSize,int concurrencyLevel) {
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

    @Override
    public Txn[] getTransactions(long[] txnIds) throws IOException {
        Txn[] txns = new Txn[txnIds.length];
        for (int i =0;i<txnIds.length ;i++)
            txns[i] = getTransaction(txnIds[i]);
        return txns;
    }

    @Override
    public void cache(Txn[] toCache) {
        for (int i = 0; i< toCache.length; i++)
            cache(toCache[i]);
    }
}
