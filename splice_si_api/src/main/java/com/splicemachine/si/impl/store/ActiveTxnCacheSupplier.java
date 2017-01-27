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

import com.splicemachine.collections.LongKeyedCache;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnSupplier;
import com.splicemachine.si.api.txn.TxnView;

import java.io.IOException;

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
public class ActiveTxnCacheSupplier implements TxnSupplier {
    private final LongKeyedCache<TxnView> cache;
		private final TxnSupplier delegate;

		public ActiveTxnCacheSupplier(TxnSupplier delegate, int maxSize) {
        this.cache = LongKeyedCache.<TxnView>newBuilder().maxEntries(maxSize)
                .withHashFunction(HashFunctions.murmur3(0)).build();
				this.delegate = delegate;
    }

		@Override
		public TxnView getTransaction(long txnId) throws IOException {
				return getTransaction(txnId,false);
		}

		@Override
		public TxnView getTransaction(long txnId, boolean getDestinationTables) throws IOException {
        TxnView txn = this.cache.get(txnId);
				if(txn!=null) return txn;
				//bummer, not cached. try delegate
				txn = delegate.getTransaction(txnId,getDestinationTables);
				if(txn==null) return null;

				if(txn.getEffectiveState()== Txn.State.ACTIVE)
            this.cache.put(txnId,txn);
				return txn;
		}

		@Override
		public boolean transactionCached(long txnId) {
        return cache.get(txnId) !=null;
		}

		@Override
    public void cache(TxnView toCache) {
        cache.put(toCache.getTxnId(),toCache);
    }

    @Override
    public TxnView getTransactionFromCache(long txnId) {
        return cache.get(txnId);
    }

    public int getSize(){
        return cache.size();
    }
}
