package com.splicemachine.si.impl;

import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnAccess;

import java.io.IOException;

/**
 * Transaction Store which constructs lazy Txn elements when needed
 * to reduce the performance cost of doing remote lookups.
 *
 * @author Scott Fines
 * Date: 6/19/14
 */
public class LazyTxnAccess implements TxnAccess {
		private final TxnAccess delegate;

		public LazyTxnAccess(TxnAccess delegate) {
				this.delegate = delegate;
		}

		@Override
		public Txn getTransaction(long txnId) throws IOException {
				/*
				 * When the delegate contains the transaction in it's local cache,
				 * it should be very inexpensive to perform a direct lookup. Therefore,
				 * we can save an extra object creation when it's contained in the cache
				 * by just delegating in that case.
				 *
				 * When it's not there, we would like to defer the lookup of the transaction,
				 * in case it's not needed (e.g. in case all values are present in the
				 * child or whatever, so defaults are never needed).
				 */
				if(delegate.transactionCached(txnId))
						return delegate.getTransaction(txnId);
				return new LazyTxn(txnId,delegate);
		}

		@Override
		public boolean transactionCached(long txnId) {
				return delegate.transactionCached(txnId);
		}

		@Override
		public void cache(Txn toCache) {
				delegate.cache(toCache);
		}
}
