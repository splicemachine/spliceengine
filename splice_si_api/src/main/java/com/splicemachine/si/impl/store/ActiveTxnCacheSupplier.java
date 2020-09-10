/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import com.splicemachine.si.api.txn.TaskId;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import splice.com.google.common.cache.Cache;
import splice.com.google.common.cache.CacheBuilder;

import java.io.IOException;

/**
 * Represents a Transaction Store which caches active transactions. This is intended for scans,
 * where a transaction may only be cached for a particular operation.
 * <p>
 * This class is only thread-safe if it's constructed with threadSafe = true
 *
 * @author Scott Fines
 * Date: 6/18/14
 */
public class ActiveTxnCacheSupplier implements TxnSupplier {
    private static final int CACHE_DEFAULT_CONCURRENCY_LEVEL = 4;
    private final Cache<Long,TxnView> cache;
    private final TxnSupplier delegate;

    public ActiveTxnCacheSupplier(TxnSupplier delegate, int initialSize, int maxSize) {
        this(delegate, initialSize, maxSize, false);
    }

    public ActiveTxnCacheSupplier(TxnSupplier delegate, int initialSize, int maxSize, boolean threadSafe) {
        this.cache = CacheBuilder.newBuilder().initialCapacity(initialSize).maximumSize(maxSize)
                .concurrencyLevel(threadSafe ? CACHE_DEFAULT_CONCURRENCY_LEVEL : 1).build();
        this.delegate = delegate;
    }

    @Override
    public TxnView getTransaction(long txnId) throws IOException {
        return getTransaction(txnId, false);
    }

    @Override
    public TxnView getTransaction(long txnId, boolean getDestinationTables) throws IOException {
        TxnView txn = this.cache.getIfPresent(txnId);
        if (txn != null) return txn;
        //bummer, not cached. try delegate
        txn = delegate.getTransaction(txnId, getDestinationTables);
        if (txn == null) return null;

        if (txn.getEffectiveState() == Txn.State.ACTIVE)
            this.cache.put(txnId, txn);
        return txn;
    }

    @Override
    public boolean transactionCached(long txnId) {
        return cache.getIfPresent(txnId) != null ? true : delegate.transactionCached(txnId);
    }

    @Override
    public void cache(TxnView toCache) {
        if (toCache.getState() == Txn.State.ACTIVE)
            cache.put(toCache.getTxnId(), toCache);
        else
            delegate.cache(toCache);
    }

    @Override
    public TxnView getTransactionFromCache(long txnId) {
        TxnView tentative = cache.getIfPresent(txnId);
        return tentative != null ? tentative : delegate.getTransactionFromCache(txnId);
    }

    @Override
    public TaskId getTaskId(long txnId) throws IOException {
        return delegate.getTaskId(txnId);
    }
}
