/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

import com.splicemachine.hash.Hash32;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.si.api.txn.TaskId;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;

import java.io.IOException;

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
    private TxnView[] cache;
    private final TxnSupplier delegate;
    private Hash32 hashFunction;

    public CompletedTxnCacheSupplier(TxnSupplier delegate, int maxSize, int concurrencyLevel) {
        cache = new TxnView[Integer.highestOneBit(maxSize)];    // ensure power of 2
        this.delegate = delegate;
        hashFunction = HashFunctions.utilHash();
    }

    private int idx(long val) {
        return hashFunction.hash(val) & (cache.length - 1);
    }

    private int idx2(long val, int idx) {
        return (int)(val + idx) & (cache.length - 1);
    }

    private TxnView get(long key) {
        int idx = idx(key);
        TxnView txn = cache[idx];   // safe to do without synchronization (JLS 17.7)
        if (txn != null && txn.getTxnId() == key) return txn;
        idx = idx2(key, idx);
        txn = cache[idx];
        return txn != null && txn.getTxnId() == key ? txn : null;
    }

    private void put(long key, TxnView txn) {
        int idx = idx(key);
        cache[idx] = txn;           // safe to do without synchronization (JLS 17.7)
        idx = idx2(key, idx);
        cache[idx] = txn;
    }

    @Override
    public TxnView getTransaction(long txnId) throws IOException{
        return getTransaction(txnId, false);
    }

    @Override
    public TxnView getTransaction(long txnId, boolean getDestinationTables) throws IOException {
        if (txnId == -1) {
            return Txn.ROOT_TRANSACTION;
        }
        TxnView transaction = get(txnId);
        if (transaction != null) {
            return transaction;
        }

        // Not in the cache, need to check the delegate
        transaction = delegate.getTransaction(txnId, getDestinationTables);
        if (transaction != null) {
            switch (transaction.getEffectiveState()) {
                case COMMITTED:
                case ROLLEDBACK:
                    put(transaction.getTxnId(), transaction); // Cache for Future Use
                    break;
                default:
                    break;
            }
        }
        return transaction;
    }

    @Override
    public boolean transactionCached(long txnId) {
        return get(txnId) != null;
    }

    @Override
    public void cache(TxnView toCache) {
        if (toCache.getState() == Txn.State.ACTIVE) return; //cannot cache incomplete transactions
        put(toCache.getTxnId(), toCache);
    }

    @Override
    public TxnView getTransactionFromCache(long txnId) {
        return get(txnId);
    }

    @Override
    public TaskId getTaskId(long txnId) throws IOException {
        return delegate.getTaskId(txnId);
    }
}
