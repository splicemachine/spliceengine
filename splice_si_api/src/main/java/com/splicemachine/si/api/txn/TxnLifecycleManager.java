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
     * Create a Child transaction of the parent, inheriting dependent, additive, and isolation level properties.
     * <p/>
     * this is functionally equivalent to calling
     * {@code beginChildTransaction(parentTxn,parentTxn.getIsolationLevel(),parentTxn.isDependent(),destinationTable,parentTxn.isAdditive())};
     *
     * @param parentTxn        the parent transaction, or {@code null} if this is a top-level transaction. If {@code null},
     *                         then the default values supplied by {@link Txn#ROOT_TRANSACTION} will be used for isolation level,
     *                         dependent, and additive properties.
     * @return a new transaction with inherited properties
     * @throws IOException if something goes wrong in creating the transaction
     */
    Txn beginChildTransaction(Txn parentTxn) throws IOException;

    /**
     * Elevate a transaction from a read-only transaction to one which allows writes. This
     * follows the lifecycle of
     * <p/>
     * 1. transaction begins in read-only state
     * 2. when writes are desired, transaction is elevated to a writable state
     * 3. Txn writes
     * 4. transaction commits/rolls back
     *
     * @param txn              the transaction to elevate
     * @throws IOException If something goes wrong during the elevation
     */
    Txn elevateTransaction(Txn txn) throws IOException;

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
    long commit(Txn txn) throws IOException;

    /**
     * Rollback the transaction identified with {@code txnId}.
     * <p/>
     * If the transaction has already been rolled back, committed, or timed out, this method will do nothing.
     *
     * @param txnId the id of the transaction to rollback
     * @throws IOException If something goes wrong during the rollback
     */
    void rollback(Txn txn) throws IOException;

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
    Txn chainTransaction(Txn parentTxn, Txn txnToCommit) throws IOException;

    /**
     * Puts this manager into Restore Mode, which would be deactivated after a reboot
     */
    void enterRestoreMode();
}
