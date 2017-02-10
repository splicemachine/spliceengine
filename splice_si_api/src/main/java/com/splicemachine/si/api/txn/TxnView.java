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

package com.splicemachine.si.api.txn;

import com.splicemachine.utils.ByteSlice;
import java.io.Externalizable;
import java.util.Iterator;

/**
 * Represents an unmodifiable view of a transaction.
 *
 * This is primarily so that, in circumstances where there
 * is no need or desire to get a modifiable transaction, we
 * can assert that directly, rather than implicitly relying on
 * code to do the right thing.
 *
 * @author Scott Fines
 * Date: 8/14/14
 */
public interface TxnView extends Externalizable {

    Txn.State getEffectiveState();

    /**
     * @return the read isolation level for this transaction.
     */
    Txn.IsolationLevel getIsolationLevel();

    /**
     * @return the unique transaction id for this transaction.
     */
    long getTxnId();

    boolean allowsSubtransactions();

    int getSubId();

    /**
     * @return the begin timestmap for the transaction.
     */
    long getBeginTimestamp();

    /**
     * Return the actual commit timestamp. If the transaction is dependent upon a parent, then this will
     * return a different timestamp than that of {@link #getEffectiveCommitTimestamp()}.
     *
     * @return the actual commit timestamp for the transaction.
     */
    long getCommitTimestamp();

    /**
     * Return the "Effective" Commit timestamp.
     *
     * If the transaction is dependent upon a parent, then this will return the commit timestamp
     * of the parent transaction. Otherwise, it will return the commit timestamp for the transaction itself.
     *
     * If the transaction has not been committed, then this will return -1;
     *
     * @return the effective commit timestamp, or -1 if the transaction should not be considered committed.
     */
    long getEffectiveCommitTimestamp();

    /**
     * Return the "Effective" Begin timestamp.
     *
     * This is necessary to deal with parent-child relationships.  To see why, consider the following case
     *
     * 1. Create parent with begin timestamp 1
     * 2. Create independent tx with begin timestamp 2
     * 3. write some data with txn 2
     * 4. commit txn 2 with commit timestamp 3.
     * 3. Create child of parent with begin timestamp 4.
     *
     * When that child reads data, his begin timestamp will occur after txn2's commit timestamp, implying
     * that the result is visible. However, the child is a child of the parent, and that result is not visible
     * to the parent (assuming the parent is in Snapshot Isolation Isolation level), so it shouldn't be visible
     * to the child either.
     *
     * This is resolved by making the child return the begin timestamp of the parent as it's "effective" begin
     * timestamp--the timestamp to be used when making these kinds of comparisons.
     *
     * @return the effective begin timestamp of the transaction. If the transaction has no parents, then its
     * effective begin timestamp is the same as it's begin timestamp.
     */
    long getEffectiveBeginTimestamp();

    /**
     * @return the last time this transaction is <em>known</em> to have been alive at, or {@code -1} if
     * that value is not known to this instance.
     *
     * In most cases, it is reasonable to expect that this value is -1--in other words, unless otherwise
     * specified, this should not be treated as reliable.
     */
    long getLastKeepAliveTimestamp();

    TxnView getParentTxnView();

    /**
     * @return the parent transaction id for this transaction, or {@code -1} if the transaction is a top-level
     * transaction.
     */
    long getParentTxnId();

    /**
     * @return the current state of the transaction
     */
    Txn.State getState();

    /**
     * @return true if this transaction allows writes
     */
    boolean allowsWrites();

    /**
     * Determine if the other transaction is visible to this transaction (e.g. this instance can "see" writes
     * made by {@code otherTxn}).
     *
     * The exact semantics of this method depends on the isolation level of the transaction:
     *
     * <ul>
     *     <li>{@link com.splicemachine.si.api.txn.Txn.IsolationLevel#READ_COMMITTED}:
     *     writes are visible if {@code otherTxn} has not been rolled back</li>
     *     <li>{@link com.splicemachine.si.api.txn.Txn.IsolationLevel#READ_COMMITTED}:
     *     writes are visible if {@code otherTxn} has been committed</li>
     *     <li>{@link com.splicemachine.si.api.txn.Txn.IsolationLevel#SNAPSHOT_ISOLATION}:
     *     writes are visible if {@code otherTxn} has been committed <em>and</em>
     *     {@code otherTxn.getEffectiveCommitTimestamp() < this.getBeginTimestamp()}
     *     </li>
     * </ul>
     *
     * Additionally, this method has distinct semantics which differ slightly:
     * <ul>
     *     <li>{@code this.equals(otherTxn)}: Writes are visible</li>
     *     <li>{@code otherTxn} is a parent of {@code this}: Writes are visible</li>
     * </ul>
     *
     * In all other circumstances, IsolationLevel semantics win.
     *
     * @param otherTxn the transaction to check
     * @return true if this transaction can see writes made by {@code otherTxn}
     */
    boolean canSee(TxnView otherTxn);

    boolean isAdditive();

    /**
     * Return the "Global Commit timestamp".
     *
     * This is a performance improvement for dependent child transactions. When this field is
     * set, it refers to the commit timestamp of this transaction's ultimate parent. For example,
     * if Transaction P has a commit timestamp of 3, and a dependent child transaction C with
     * a commit timestamp of 2, then {@code C.getGlobalCommitTimestamp() = 3};
     *
     * If this transaction is a top-level transaction (e.g. an immediate child of
     * {@link Txn#ROOT_TRANSACTION}), then this will return the commit timestamp, or -1 if it does
     * not have a commit timestamp.
     *
     * @return the global commit timestamp
     */
    long getGlobalCommitTimestamp();

    ConflictType conflicts(TxnView otherTxn);

    Iterator<ByteSlice> getDestinationTables();

    /**
     * Determine if this transaction is a descendent of the specified transaction.
     *
     * @param potentialParent the transaction which may be the parent.
     * @return true if this transaction is a descendent of the specified transaction.
     * @throws  java.lang.NullPointerException if {@code potentialParent==null}.
     */
    boolean descendsFrom(TxnView potentialParent);
    
}
