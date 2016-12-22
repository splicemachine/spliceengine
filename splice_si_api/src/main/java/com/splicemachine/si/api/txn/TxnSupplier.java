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
		 * @param txnId the transaction id to fetch.
		 */
		Txn getTransaction(long txnIds) throws IOException;


		/**
		 * Get the transaction associated with {@code txnId}.
		 *
		 * @param txnId the transaction id to fetch.
		 */
		Txn[] getTransactions(long[] txnIds) throws IOException;

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
		void cache(Txn toCache);

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
		void cache(Txn[] toCache);

}
