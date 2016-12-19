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

package com.splicemachine.si.api.txn;


import java.io.IOException;

/**
 * @author Scott Fines
 * Date: 6/18/14
 */
public interface TxnSupplier {

		/**
		 * Get the transaction associated with {@code txnId}.
		 *
		 * Functionally equivalent to {@link #getTransaction(long, boolean)} with
		 * {@code getDestinationTables = false}.
		 *
		 * @param txnId the transaction id to fetch.
		 */
		Transaction getTransaction(long txnId) throws IOException;

		/**
		 * Determines whether this Store has the transaction in its local cache
		 * or not.
		 *
		 * If the Store does not have a local cache, then this call should always
		 * return false.
		 *
		 * This method only requires <em>best-guess</em> semantics--it does <em>not</em>
		 * require absolute correctness, nor does it place any implicit guarantees on subsequent
		 * {@link #getTransaction(long)} calls.
		 *
		 * This method returning true does <em>not</em> guarantee that subsequent
		 * calls to {@link #getTransaction(long)} will not incur additional costs.
		 * It is possible (particularly in concurrent stores) that the transaction may be evicted
		 * from the cache between the time when this method returns and {@code getTransaction(long)}
		 * is called.
		 *
		 * Similarly, just because this method returns {@code false} does <em>not</em> guarantee
		 * that subsequent calls to {@code getTransaction(long)} will not find the element in its cache--
		 * It is possible (particularly in concurrent stores) that the transaction may be loaded into
		 * the cache between the time when this method returns and {@code getTransaction(long)} is called.
		 *
		 *
		 * @param txnId the transaction id to fetch.
		 * @return true if the transaction is held in the local cache (and is therefore
		 * inexpensive to lookup).
		 */
		boolean transactionCached(long txnId);

		/**
		 * Add the transaction to the local cache (if such a cache exists).
		 *
		 * If the implementation does not cache Transactions, then this method does nothing.
		 *
		 * If a transaction with the same id already exists in the cache, this method does nothing.
		 *
		 * If the transaction specified is null, an exception may be thrown.
		 *
		 * @param toCache the transaction to cache.
		 */
		void cache(TxnView toCache);

    TxnView getTransactionFromCache(long txnId);
}
