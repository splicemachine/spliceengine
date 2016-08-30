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

import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import org.sparkproject.guava.cache.Cache;
import org.sparkproject.guava.cache.CacheBuilder;
import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 9/5/14
 */
public class SimpleCompletedTxnCacheSupplier implements TxnSupplier{
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

    public long getTotalEvictedEntries() { return cache.stats().evictionCount(); }
    public long getTotalHits() { return cache.stats().hitCount(); }
    public long getTotalMisses() { return cache.stats().missCount(); }
    public long getTotalRequests() { return cache.stats().requestCount(); }
    public float getHitPercentage() { return (float)cache.stats().hitRate(); }
    public int getCurrentSize() { return (int)cache.size(); }

    public int getMaxSize() {
        return maxSize;
    }
}
