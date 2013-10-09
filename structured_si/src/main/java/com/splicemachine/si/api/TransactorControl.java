package com.splicemachine.si.api;

import com.splicemachine.si.impl.TransactionId;

import java.io.IOException;
import java.util.List;

/**
 * Transactor calls for controlling transactions (e.g. starting, stopping them).
 */
public interface TransactorControl {
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
    TransactionId beginChildTransaction(TransactionId parent, boolean dependent, boolean allowWrites,
                                        Boolean readUncommitted, Boolean readCommitted) throws IOException;
    void keepAlive(TransactionId transactionId) throws IOException;
    void commit(TransactionId transactionId) throws IOException;
    void rollback(TransactionId transactionId) throws IOException;
    void fail(TransactionId transactionId) throws IOException;
    TransactionStatus getTransactionStatus(TransactionId transactionId) throws IOException;

    TransactionId transactionIdFromString(String transactionId);

    List<TransactionId> getActiveTransactionIds(TransactionId max) throws IOException;
    boolean forbidWrites(String tableName, TransactionId transactionId) throws IOException;
}
