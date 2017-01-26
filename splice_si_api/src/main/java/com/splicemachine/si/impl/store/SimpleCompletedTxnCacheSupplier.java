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

import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import org.spark_project.guava.cache.Cache;
import org.spark_project.guava.cache.CacheBuilder;
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
