package com.splicemachine.si.impl.store;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.splicemachine.si.api.TransactionCacheManagement;
import com.splicemachine.si.api.TxnSupplier;
import com.splicemachine.si.api.TxnView;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 9/5/14
 */
public class SimpleCompletedTxnCacheSupplier implements TxnSupplier,TransactionCacheManagement{
    private final Cache<Long,TxnView> cache;
    private final TxnSupplier delegate;
    private final int maxSize;

    public SimpleCompletedTxnCacheSupplier(TxnSupplier delegate,int maxSize,int concurrencyLevel) {
        this.delegate = delegate;
        this.cache =  CacheBuilder.newBuilder().maximumSize(maxSize)
                .softValues().concurrencyLevel(concurrencyLevel).build();
        this.maxSize = maxSize;
    }

    @Override
    public TxnView getTransaction(final long txnId) throws IOException {
        return getTransaction(txnId,false);
    }

    @Override
    public TxnView getTransaction(final long txnId,final  boolean getDestinationTables) throws IOException {
        try {
            return cache.get(txnId,new Callable<TxnView>() {
                @Override
                public TxnView call() throws Exception {
                    return delegate.getTransaction(txnId,getDestinationTables);
                }
            });
        } catch (ExecutionException e) {
            throw (IOException)e.getCause();
        }
    }

    @Override
    public boolean transactionCached(long txnId) {
        return getTransactionFromCache(txnId)!=null;
    }

    @Override
    public void cache(TxnView toCache) {
        cache.put(toCache.getTxnId(),toCache);
    }

    @Override
    public TxnView getTransactionFromCache(long txnId) {
        return cache.getIfPresent(txnId);
    }

    @Override
    public long getTotalEvictedEntries() {
        return cache.stats().evictionCount();
    }

    @Override
    public long getTotalHits() {
        return cache.stats().hitCount();
    }

    @Override
    public long getTotalMisses() {
        return cache.stats().missCount();
    }

    @Override
    public long getTotalRequests() {
        return cache.stats().requestCount();
    }

    @Override
    public float getHitPercentage() {
        return (float)cache.stats().hitRate();
    }

    @Override
    public int getCurrentSize() {
        return (int)cache.size();
    }

    @Override
    public int getMaxSize() {
        return maxSize;
    }
}
