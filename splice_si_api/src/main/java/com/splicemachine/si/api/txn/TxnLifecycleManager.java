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

package com.splicemachine.si.api.txn;

import com.carrotsearch.hppc.LongHashSet;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 6/18/14
 */
public interface TxnLifecycleManager{

    /**
     * Begin a top-level read-only transaction.
     * <p/>
     * This is functionally equivalent to calling
     * {@code beginTransaction(Txn.ROOT_TRANSACTION.getIsolationLevel(),false,false,Txn.ROOT_TRANSACTION,null)}
     *
     * @return a top-level read-only transaction.
     * @throws java.io.IOException if something goes wrong in creating the transaction
     */
    Txn beginTransaction() throws IOException;

    /**
     * Begin a top-level read-only transaction.
     * <p/>
     * This is functionally equivalent to calling
     * {@code beginTransaction(Txn.ROOT_TRANSACTION.getIsolationLevel(),false,false,Txn.ROOT_TRANSACTION,destinationTable)}
     *
     * @param destinationTable a table to which writes are to proceed, or {@code null} if the transaction
     *                         is to start as read-only.
     * @return a top-level read-only transaction.
     * @throws java.io.IOException if something goes wrong in creating the transaction
     */
    Txn beginTransaction(byte[] destinationTable) throws IOException;

    /**
     * Begin a top-level read-only transaction.
     * <p/>
     * the returned transaction can be elevated to a writable transaction by using the methods
     * located on the returned object.
     * <p/>
     * This is functionally equivalent to calling
     * {@code beginTransaction(isolationLevel,false,false,Txn.ROOT_TRANSACTION,null)}
     *
     * @param isolationLevel the isolation level to use for reads
     * @return a top-level read-only transaction.
     * @throws java.io.IOException if something goes wrong in creating the transaction
     */
    Txn beginTransaction(Txn.IsolationLevel isolationLevel) throws IOException;

    /**
     * Begin a top-level read-only transaction.
     * <p/>
     * the returned transaction can be elevated to a writable transaction by using the methods
     * located on the returned object.
     * <p/>
     * This is functionally equivalent to calling
     * {@code beginTransaction(isolationLevel,false,false,Txn.ROOT_TRANSACTION,destinationTable)}
     *
     * @param isolationLevel   the isolation level to use for reads
     * @param destinationTable a table to which writes are to proceed, or {@code null} if the transaction
     *                         is to start as read-only.
     * @return a top-level read-only transaction.
     * @throws java.io.IOException if something goes wrong in creating the transaction
     */
    Txn beginTransaction(Txn.IsolationLevel isolationLevel,byte[] destinationTable) throws IOException;

    /**
     * Create a Child transaction of the parent, inheriting dependent, additive, and isolation level properties.
     * <p/>
     * this is functionally equivalent to calling
     * {@code beginChildTransaction(parentTxn,parentTxn.getIsolationLevel(),parentTxn.isDependent(),destinationTable,parentTxn.isAdditive())};
     *
     * @param parentTxn        the parent transaction, or {@code null} if this is a top-level transaction. If {@code null},
     *                         then the default values supplied by {@link Txn#ROOT_TRANSACTION} will be used for isolation level,
     *                         dependent, and additive properties.
     * @param destinationTable a table to which writes are to proceed, or {@code null} if the transaction
     *                         is to start as read-only.
     * @return a new transaction with inherited properties
     * @throws IOException if something goes wrong in creating the transaction
     */
    Txn beginChildTransaction(TxnView parentTxn,byte[] destinationTable) throws IOException;

    /**
     * Create a Child transaction of the parent, inheriting dependent and additive properties.
     * <p/>
     * this is functionally equivalent to calling
     * {@code beginChildTransaction(parentTxn,isolationLevel,parentTxn.isDependent(),parentTxn.isAdditive(),destinationTable)};
     *
     * @param parentTxn        the parent transaction, or {@code null} if this is a top-level transaction. If {@code null},
     *                         then the default values supplied by {@link Txn#ROOT_TRANSACTION} will be used for isolation level,
     *                         dependent, and additive properties.
     * @param isolationLevel   the isolation level to use for reads
     * @param destinationTable a table to which writes are to proceed, or {@code null} if the transaction
     *                         is to start as read-only.
     * @return a new transaction with inherited properties
     * @throws IOException if something goes wrong in creating the transaction
     */
    Txn beginChildTransaction(TxnView parentTxn,Txn.IsolationLevel isolationLevel,byte[] destinationTable) throws IOException;

    /**
     * Begin a child transaction of the parent.
     *
     * @param parentTxn        the parent transaction, or {@code null} if this is a top-level transaction
     * @param isolationLevel   the isolation level to use for reads
     * @param additive         If this is a write transaction, whether it is considered "additive". If {@code true}, then
     *                         this transaction will not throw Write/Write conflicts. If {@code destinationTable==null},
     *                         then this is carried through until a writable transaction is created.
     * @param destinationTable a table to which writes are to proceed, or {@code null} if the transaction
     *                         is to start as read-only.
     * @return a new child transaction
     * @throws java.io.IOException if something goes wrong in creating the transaction
     */
    Txn beginChildTransaction(TxnView parentTxn,
                              Txn.IsolationLevel isolationLevel,
                              boolean additive,
                              byte[] destinationTable) throws IOException;

    /**
     * Begin a child transaction of the parent.
     *
     * @param parentTxn        the parent transaction, or {@code null} if this is a top-level transaction
     * @param isolationLevel   the isolation level to use for reads
     * @param additive         If this is a write transaction, whether it is considered "additive". If {@code true}, then
     *                         this transaction will not throw Write/Write conflicts. If {@code destinationTable==null},
     *                         then this is carried through until a writable transaction is created.
     * @param destinationTable a table to which writes are to proceed, or {@code null} if the transaction
     *                         is to start as read-only.
     * @param inMemory         whether to create an in memory subtransaction or not. Even if it's true we might create
     *                         a persisted transaction if condition aren't met
     * @return a new child transaction
     * @throws java.io.IOException if something goes wrong in creating the transaction
     */
    Txn beginChildTransaction(TxnView parentTxn,
                              Txn.IsolationLevel isolationLevel,
                              boolean additive,
                              byte[] destinationTable,
                              boolean inMemory) throws IOException;

    /**
     * Begin a child transaction of the parent.
     *
     * @param parentTxn        the parent transaction, or {@code null} if this is a top-level transaction
     * @param isolationLevel   the isolation level to use for reads
     * @param additive         If this is a write transaction, whether it is considered "additive". If {@code true}, then
     *                         this transaction will not throw Write/Write conflicts. If {@code destinationTable==null},
     *                         then this is carried through until a writable transaction is created.
     * @param destinationTable a table to which writes are to proceed, or {@code null} if the transaction
     *                         is to start as read-only.
     * @param inMemory         whether to create an in memory subtransaction or not. Even if it's true we might create
     *                         a persisted transaction if condition aren't met
     * @param taskId           task identifier so that we can ignore conflicts with previous retries of the same task
     * @return a new child transaction
     * @throws java.io.IOException if something goes wrong in creating the transaction
     */
    Txn beginChildTransaction(TxnView parentTxn,
                              Txn.IsolationLevel isolationLevel,
                              boolean additive,
                              byte[] destinationTable,
                              boolean inMemory,
                              TaskId taskId) throws IOException;

    /**
     * Elevate a transaction from a read-only transaction to one which allows writes. This
     * follows the lifecycle of
     * <p/>
     * 1. transaction begins in read-only state
     * 2. when writes are desired, transaction is elevated to a writable state
     * 3. Transaction writes
     * 4. transaction commits/rolls back
     *
     * @param txn              the transaction to elevate
     * @param destinationTable the destination table where modifications are to occur.
     *                         This serves as an effective DDL lock on that particular table--i.e.
     *                         no DDL operation can proceed while this transaction is active, because
     *                         it may have written data to that location (Even if this transaction represents
     *                         a DDL activity).
     * @throws IOException If something goes wrong during the elevation
     */
    Txn elevateTransaction(Txn txn,byte[] destinationTable) throws IOException;

    /**
     * Commit the specified transaction id.
     * <p/>
     * If the transaction has already been rolled back or timedout, then this method will throw an exception.
     *
     * @param txnId the id of the transaction to commit.
     * @return the commit timestamp for the committed transaction.
     * @throws com.splicemachine.si.api.CannotCommitException if the transaction was already rolled back by
     *                                                        another process (e.g. timeout)
     * @throws IOException                                    if something goes wrong during the elevation
     */
    long commit(long txnId) throws IOException;

    /**
     * Rollback the transaction identified with {@code txnId}.
     * <p/>
     * If the transaction has already been rolled back, committed, or timed out, this method will do nothing.
     *
     * @param txnId the id of the transaction to rollback
     * @throws IOException If something goes wrong during the rollback
     */
    void rollback(long txnId) throws IOException;

    void rollbackSubtransactions(long txnId, LongHashSet rolledback) throws IOException;

    void unregisterActiveTransaction(long txnId) throws IOException;

    /**
     * "Chains" a new transaction to the old one.
     * <p/>
     * "Chaining" is when one transaction is committed, and the commit timestamp that was generated
     * for that transaction is used as the begin timestamp of the next transaction. It is used
     * when a new transaction is desired, but no breaks in logical time are acceptable (such as during
     * a DDL or other form of operation where a clear demarcation of times are required).
     *
     * @param parentTxn        the parent transaction, or {@code null} if this is a top-level transaction
     * @param isolationLevel   the isolation level to use for reads
     * @param additive         if the new transaction is to be additive.
     * @param destinationTable a table to which writes are to proceed, or {@code null} if the transaction
     *                         is to start as read-only.
     * @param txnToCommit      the transaction to commit.
     * @return a new transaction whose begin timestamp is the same as the commit timestamp of {@code txnToCommit}
     */
    Txn chainTransaction(TxnView parentTxn,
                         Txn.IsolationLevel isolationLevel,
                         boolean additive,
                         byte[] destinationTable,Txn txnToCommit) throws IOException;

    /**
     * Puts this manager into Restore Mode, which would be deactivated after a reboot
     */
    void enterRestoreMode();

    boolean isRestoreMode();

    void setReplicationRole(String role);

    String getReplicationRole();
}
