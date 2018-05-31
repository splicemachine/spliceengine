/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
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

import com.splicemachine.collections.LongKeyedCache;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.si.api.txn.TaskId;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.txn.AbstractTxnStore;
import java.io.IOException;
import java.util.HashMap;
import java.util.Set;

/**
 * Represents a Transaction Store which caches active transactions. This is intended for scans,
 * where a transaction may only be cached for a particular operation.
 *
 * This class is <em>not</em> thread-safe--only a single thread may use this at a time without
 * external synchronization. However, as a single transaction should be represented using a single thread
 * anyway, this class doesn't make sense to be thread safe anyway.
 *
 * @author Scott Fines
 * Date: 6/18/14
 */
public class ActiveTxnCacheSupplier extends AbstractTxnStore implements TxnSupplier {
    private final LongKeyedCache<TxnView> cache;
		private final TxnSupplier delegate;

		public ActiveTxnCacheSupplier(TxnSupplier delegate) {
			this(delegate,1024);
		}
		public ActiveTxnCacheSupplier(TxnSupplier delegate, int maxSize) {
        this.cache = LongKeyedCache.<TxnView>newBuilder().maxEntries(maxSize)
                .withHashFunction(HashFunctions.murmur3(0)).build();
				this.delegate = delegate;
    }

	@Override
	public HashMap<Long, TxnView> getTransactions(TxnView currentTxn, Set<Long> txnIds) throws IOException {
		HashMap<Long, TxnView> txns = new HashMap<>(txnIds.size());
		for (long txnId: txnIds) {
			txns.put(txnId,getTransaction(currentTxn,txnId));
		}
		return txns;
	}

		@Override
		public boolean transactionCached(long txnId) {
        	return cache.get(txnId) !=null ? true : delegate.transactionCached(txnId);
		}

		@Override
   		public void cache(TxnView toCache) {
		    if (toCache.getState() == Txn.State.ACTIVE)
                cache.put(toCache.getTxnId(),toCache);
		    else
		        delegate.cache(toCache);
        }

		@Override
		public TxnView getTransactionFromCache(long txnId) {
			TxnView tentative = cache.get(txnId);
			return tentative != null ? tentative : delegate.getTransactionFromCache(txnId);
		}

	@Override
	public TaskId getTaskId(long txnId) throws IOException {
		return delegate.getTaskId(txnId);
	}

	public int getSize(){
        return cache.size();
    }

	@Override
	public HashMap<Long, TxnView> getBaseTransactions(TxnView currentTxn, Set<Long> txnIds) throws IOException {
		HashMap<Long, TxnView> txns = new HashMap<>(txnIds.size());
		for (long txnId: txnIds) {
			txns.put(txnId,getBaseTransaction(currentTxn, txnId));
		}
		return txns;
	}

	@Override
	public TxnView getBaseTransaction(TxnView currentTxn, long txnId, boolean getDestinationTables) throws IOException {
		TxnView txn = this.cache.get(txnId);
		if(txn!=null) return txn;
		//bummer, not cached. try delegate
		txn = delegate.getBaseTransaction(currentTxn,txnId,getDestinationTables);
		if(txn==null) return null;

		if(txn.getEffectiveState()== Txn.State.ACTIVE)
			this.cache.put(txnId,txn);
		return txn;
	}

	@Override
	public TxnView getTransaction(TxnView currentTxn, long txnId, boolean getDestinationTables) throws IOException {
		return super.getTransaction(currentTxn, txnId, getDestinationTables);
	}
}
