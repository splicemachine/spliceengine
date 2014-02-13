package com.splicemachine.si.api;

import com.splicemachine.si.impl.TransactionId;

import java.io.IOException;
import java.util.List;

/**
 * Transactor calls for controlling transactions (e.g. starting, stopping them).
 */
public interface TransactionManager {
    /**
     * Start a writable transaction in "snapshot isolation" concurrency mode.
     */
    TransactionId beginTransaction() throws IOException;

    /**
     * Start a transaction in "snapshot isolation" concurrency mode.
     */
    TransactionId beginTransaction(boolean allowWrites) throws IOException;

    /**
     * Start a transaction with the specified isolation mode.
     * @param allowWrites boolean indicator of whether the new transaction is allowed to write
     * @param readUncommitted indicator of whether to read data from other, uncommitted transactions
     * @param readCommitted indicator of whether to read data from committed transactions that occur after this new
     *                      transaction is begun
     */
    TransactionId beginTransaction(boolean allowWrites, boolean readUncommitted, boolean readCommitted) throws IOException;
    TransactionId beginChildTransaction(TransactionId parent, boolean allowWrites) throws IOException;

    /**
     *
     *
     * @param parent transaction that contains this new transaction
     * @param dependent indicator of whether this transaction can only finally commit if the parent does
     * @param allowWrites indicates whether this transaction can perform writes
     * @return
     * @throws java.io.IOException
     */
    TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites) throws IOException;
    /**
     * @param parent              The new transaction should be a child of parent.
     * @param dependent           Whether the commit of the child depends on the parent committing as well.
     * @param allowWrites         Whether writes can be performed in the new transaction (else it is read only)
     * @param additive            Whether the new transaction will be used to perform operations that do not incur write conflicts
     *                            and which are idempotent.
     * @param readUncommitted     Whether the new transaction should see uncommitted changes from other transactions, setting
     *                            this to null causes the new transaction to "inherit" this property from its parent.
     * @param readCommitted       Whether the new transaction should see committed changes from concurrent transactions. Otherwise
     *                            snapshot isolation semantics apply. Setting this to null causes the new transaction to "inherit"
     *                            this property from its parent.
     * @param transactionToCommit The transactionToCommit is committed before starting a new transaction and the commit
     *                            timestamp is used as the begin timestamp of the new transaction. This allows a transaction
     *                            to be committed and a new one begun without any gap in the timestamps between them.
     * @return a TransactionId representing a transaction which is the child of the specified parent.
     * @throws IOException
     */
    TransactionId beginChildTransaction(TransactionId parent, boolean dependent,
                                        boolean allowWrites, boolean additive, Boolean readUncommitted, Boolean readCommitted, TransactionId transactionToCommit) throws IOException;
    void keepAlive(TransactionId transactionId) throws IOException;
    void commit(TransactionId transactionId) throws IOException;
    void rollback(TransactionId transactionId) throws IOException;
    void fail(TransactionId transactionId) throws IOException;
    TransactionStatus getTransactionStatus(TransactionId transactionId) throws IOException;

    TransactionId transactionIdFromString(String transactionId);

    List<TransactionId> getActiveTransactionIds(TransactionId max) throws IOException;
    boolean forbidWrites(String tableName, TransactionId transactionId) throws IOException;

}
