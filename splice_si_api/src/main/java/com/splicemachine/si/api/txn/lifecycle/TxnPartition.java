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

package com.splicemachine.si.api.txn.lifecycle;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.utils.Source;

import javax.annotation.concurrent.ThreadSafe;
import java.io.IOException;

/**
 * Represents a Partition of the Transaction table. This is mainly for internal usage of the
 * SI subsystem, but each different architecture will need to implement it in the most coherent
 * way possible for that architecture.
 *
 * @author Scott Fines
 *         Date: 12/14/15
 */
@ThreadSafe
public interface TxnPartition{
    /**
     * Write the full transaction information to the local storage.
     *
     * @param txn the transaction to write
     * @throws IOException if something goes wrong in writing the transaction
     */
    void recordTransaction(TxnMessage.TxnInfo txn) throws IOException;

    /**
     * Add a destination table to the transaction.
     * <p/>
     * This method is <em>not</em> thread-safe--it <em>must</em> be synchronized externally
     * in order to avoid race conditions and erroneous state conditions.
     *
     * @param txnId            the transaction id to add a destination table to
     * @param destTable the destination table to record
     * @throws IOException if something goes wrong
     */
    void addDestinationTable(long txnId,byte[] destTable)throws IOException;

    /**
     * Fetch all information about a transaction.
     *
     * @param txnId the transaction id to fetch
     * @return all recorded transaction information for the specified transaction.
     * @throws IOException if something goes wrong when fetching transactions
     */
    TxnMessage.Txn getTransaction(long txnId) throws IOException;

    /**
     * Get a list of transaction ids which are considered ACTIVE <em>at the time that they are visited</em>.
     * <p/>
     * This call does not require explicit synchronization, but it will <em>not</em> return a perfect view
     * of the table at call time. It is possible for a transaction to be added after this scan has passed (resulting
     * in missed active transactions); it is also possible for a transaction to be returned as ACTIVE but which
     * has subsequently been moved to ROLLEDBACK or COMMITTED. It is thus imperative for callers to understand
     * this requirements and operate accordingly (generally by using the transaction timestamps to prevent caring
     * about transactions outside of a particular bound).
     *
     * @param beforeTs         the upper limit on transaction timestamps to return. Transactions with a begin timestamp >=
     *                         {@code beforeTs} will not be returned.
     * @param afterTs          the lower limit on transaction timestamps to return. Transactions with a begin
     *                         timestamp < {@code afterTs} will not be returned.
     * @param destinationTable the table which the transaction must have been writing to, or {@code null} if all
     *                         active transactions in the range are to be returned.
     * @return a listing of all active write transactions between {@code afterTs} and {@code beforeTs}, and which
     * optionally write to {@code destinationTable}
     * @throws IOException
     */
    long[] getActiveTxnIds(long afterTs,long beforeTs,byte[] destinationTable) throws IOException;

    Source<TxnMessage.Txn> getActiveTxns(long startId,long endId,byte[] destTable) throws IOException;

    /**
     * Gets the current state of the transaction.
     * <p/>
     * If the transaction has been manually set to either COMMITTED or ROLLEDBACK, then this will
     * return that setting (e.g. COMMITTED or ROLLEDBACK). If the transaction is set to the ACTIVE
     * state, then this will also check the keep alive timestamp. If the time since the last keep alive
     * timestamp has exceeded the maximum window (some multiple of the configured setting, to allow for network latency),
     * then this will "convert" the transaction to ROLLEDBACK--e.g. it will not write any data, but it will
     * return a ROLLEDBACK state for the transaction instead of ACTIVE.
     *
     * @param txnId the transaction id to get state for
     * @return the current state of this transaction, or {@code null} if the transaction is not listed (e.g. it's
     * a Read-only transaction)
     * @throws IOException if something goes wrong fetching the transaction
     */
    Txn.State getState(long txnId) throws IOException;

    /**
     * Get the actual commit timestamp of the transaction (e.g. not it's effective timestamp), or {@code -1l}
     * if the transaction is still considered active, or is a read-only transaction.
     *
     * @param txnId the transaction id to acquire the recorded commit timestamp.
     * @return the commit timestamp for the specified transaction
     * @throws IOException if something goes wrong during the fetch
     */
    long getCommitTimestamp(long txnId) throws IOException;

    /**
     * @param txnId the id of the transaction which cannot commit
     * @param state the actual state of that transaction
     * @return an IOException representing an inability to Commit. Generally, this implements the
     * {@link CannotCommitException} interface as well. This is necessary in order to generate an
     * architecture-specific error which can be propagated correctly through the system.
     */
    IOException cannotCommit(long txnId,Txn.State state);

    /**
     * Record that the transaction was committed, and assign the committed timestamp to it.
     * <p/>
     * If this method returns successfully, then the transaction can safely be considered committed,
     * even if a later network call fails and forces a retry.
     * <p/>
     * Calling this method twice will have no effect on correctness <em>as long as there are not concurrent
     * rollbacks being called.</em>. As such it is vitally important that this method be called from within
     * external synchronization.
     *
     * @param txnId    the transaction id to commit
     * @param commitTs the timestamp at which the commit is said to occur
     * @throws IOException if something goes wrong while committing.
     */
    void recordCommit(long txnId,long commitTs) throws IOException;

    /**
     * Record that the transaction was globally committed, and assign the committed timestamp to it.
     * <p/>
     * This method is <em>not</em> to be used as part of the normal lifecycle of a transaction. Instead,
     * it should be treated as allowing an optimization of the transaction graph navigation--that is, if
     * the transaction isn't <em>already</em> globally committed, then using this method will result
     * in a corruption of the database. So use with care!
     * <p/>
     *
     * @param txnId    the transaction id to commit
     * @param globalCommitTs the timestamp at which the commit is said to occur
     * @throws IOException if something goes wrong while committing.
     */
    void recordGlobalCommit(long txnId, long globalCommitTs) throws IOException;

    /**
     * Record the transaction as rolled back.
     * <p/>
     * This call is <em>not</em> thread-safe and it does <em>not</em> validate that the state of the
     * transaction is active when it writes it--it's up to callers to ensure that that is true.
     * <p/>
     * However, once a call to this method is completed, then the transaction <em>must</em> be considered
     * to be rolled back (and it will in any subsequent GETs).
     *
     * @param txnId the transaction id to roll back
     * @throws IOException if something goes wrong during the write.
     */
    void recordRollback(long txnId) throws IOException;

    /**
     * Update the Transaction's keepAlive field so that the transaction is known to still be active.
     * <p/>
     * This operation must occur under a lock to ensure that reads don't occur until after the transaction
     * keep alive has been kept--otherwise, there is a race condition where some transactions may see
     * a transaction as timed out, then have a keep alive come in and make it active again.
     *
     * @param txnId the transaction id to keep alive
     * @return true if keep alives should continue (e.g. the transaction is still active)
     * @throws IOException if something goes wrong, or if the keep alive comes after
     *                     the timeout threshold
     */
    boolean keepAlive(long txnId) throws IOException;

    void rollbackTransactionsAfter(long txnId) throws IOException;

    void recordRollbackSubtransactions(long txnId, long[] subIds) throws IOException;
}
