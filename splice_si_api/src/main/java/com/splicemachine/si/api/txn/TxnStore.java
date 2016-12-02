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

import com.carrotsearch.hppc.LongOpenHashSet;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 6/24/14
 */
public interface TxnStore extends TxnSupplier{

    /**
     * Write the Transaction to underlying storage.
     *
     * @param txn the transaction to write.
     * @throws IOException if something goes wrong trying to write it
     */
    void recordNewTransaction(Txn txn) throws IOException;

    void rollback(long txnId) throws IOException;

    void rollbackSubtransactions(long txnId, LongOpenHashSet subtransactions) throws IOException;

    long commit(long txnId) throws IOException;

    boolean keepAlive(long txnId) throws IOException;

    void elevateTransaction(Txn txn,byte[] newDestinationTable) throws IOException;

    /**
     * Get a list of active write transactions. Only transactions which write
     * are guaranteed to be returned (although some implementations may opt to return
     * transactions which are read-only if they so desire).
     *
     * @param txn   the transaction with the maximum id to return. Only transactions
     *              which have a transaction id <= {@code txn.getTxnId()} will be returned. If
     *              {@code txn ==null}, then all write transactions will be returned.
     * @param table the table to limit, or {@code null} if all write transactions are to be
     *              returned. If the table is not null, then only transactions which are affecting
     *              the specified table will be returned.
     * @return all write transaction ids (optionally, some read-only transactions as well) which
     * are <= {@code txn.getTxnId()}. If {@code txn} is null, then all write transactions
     * will be returned.
     * @throws IOException
     */
    long[] getActiveTransactionIds(Txn txn,byte[] table) throws IOException;

    long[] getActiveTransactionIds(long minTxnId,long maxTxnId,byte[] table) throws IOException;

    List<TxnView> getActiveTransactions(long minTxnid,long maxTxnId,byte[] table) throws IOException;

    /**
     * @return a count of the total number of store lookups made since the server last started
     */
    long lookupCount();

    /**
     * @return a count of the total number of transactions elevated since the server last started
     */
    long elevationCount();

    /**
     * @return a count of the total number of writable transactions created since the server last started
     */
    long createdCount();

    /**
     * @return a count of the total number of transaction rollbacks made since the server last started
     */
    long rollbackCount();

    /**
     * @return a count of the total number of transaction commits made since the server last started
     */
    long commitCount();

    void setCache(TxnSupplier cache);

}
