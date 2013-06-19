package com.splicemachine.si.impl;

import java.util.Set;

import static com.splicemachine.si.impl.TransactionStatus.ACTIVE;
import static com.splicemachine.si.impl.TransactionStatus.COMMITTED;
import static com.splicemachine.si.impl.TransactionStatus.COMMITTING;
import static com.splicemachine.si.impl.TransactionStatus.ERROR;
import static com.splicemachine.si.impl.TransactionStatus.ROLLED_BACK;

/**
 * Represents an application level transaction that spans many atomic writes to the underlying data store (i.e. HBase).
 */
public class Transaction extends ImmutableTransaction {
    public static final long ROOT_ID = -1;
    static final Transaction rootTransaction = new Transaction(RootTransactionBehavior.instance, ROOT_ID, 0, 0, null, true,
            null, false, false, false, TransactionStatus.ACTIVE, null, null, null);

    /**
     * the parent transaction or null if this is a root level transaction
     */
    private final Transaction parent;

    public final TransactionStatus status;

    /**
     * the last time keepAlive was called on this transaction (or when the transaction began). This is an actual timestamp
     * (i.e. number of ms since Jan 1, 1970), not a sequence number.
     */
    public final long keepAlive;

    /**
     * the time when the transaction committed or null if it has not committed
     */
    public final Long commitTimestamp;
    public final Long globalCommitTimestamp;

    /**
     * all of the direct child transactions of this transaction
     */
    private final Set<Long> children;

    public final Long counter;

    public Transaction(TransactionBehavior transactionBehavior, long id, long beginTimestamp, long keepAlive, Transaction parent,
                       boolean dependent, Set<Long> children,
                       boolean allowWrites, Boolean readUncommitted, Boolean readCommitted, TransactionStatus status,
                       Long commitTimestamp, Long globalCommitTimestamp, Long counter) {
        super(transactionBehavior, id, allowWrites, readCommitted, parent, dependent, readUncommitted,
                beginTimestamp);
        this.keepAlive = keepAlive;
        this.parent = parent;
        this.children = children;
        this.commitTimestamp = commitTimestamp;
        this.globalCommitTimestamp = globalCommitTimestamp;
        this.status = status;
        this.counter = counter;
    }

    // immediate functions - These functions are based solely on the transaction's immediate state (i.e. not on the
    // state of their parent)

    /**
     * @return indicator of whether the transaction is currently in the COMMITTING status. This means it is in the
     *         process of obtaining a commit timestamp.
     */
    public boolean isCommitting() {
        return statusIsOneOf(COMMITTING);
    }

    /**
     * @return indicator of whether this transaction has been finally committed. Note: if a nested dependent transaction
     *         is locally committed, but its parent has not yet committed then this will return false.
     */
    public boolean isCommitted() {
        return commitTimestamp != null;
    }

    /**
     * @return indicator of whether this transaction has completed, but not successfully. This means it either errored
     *         out or was rolled back by the user.
     */
    public boolean isFailed() {
        return statusIsOneOf(ERROR, ROLLED_BACK);
    }

    private boolean statusIsOneOf(TransactionStatus... statuses) {
        for (TransactionStatus s : statuses) {
            if (s.equals(status)) {
                return true;
            }
        }
        return false;
    }

    // effective functions - these functions walk the transaction ancestry to produce their answer. So they are based
    // not just on the given transaction's state, but also on its ancestors' state.

    /**
     * @return status of this transaction or the status of it's ancestor. Transactions inherit their parent's status
     *         if they don't have one explicitly set for themselves.
     */
    public TransactionStatus getEffectiveStatus() {
        return getEffectiveStatus(Transaction.getRootTransaction());
    }

    public TransactionStatus getEffectiveStatus(ImmutableTransaction scope) {
        if (((status.equals(ACTIVE) || status.equals(COMMITTED))
                && !parent.isRootTransaction())
                && !sameTransaction(scope)) {
            return parent.getEffectiveStatus(scope);
        }
        return status;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    /**
     * @return true if this transaction is still active, based on it's parent status.
     */
    public boolean isEffectivelyActive() {
        return getEffectiveStatus().equals(ACTIVE);
    }

    ////

    public static Transaction getRootTransaction() {
        return rootTransaction;
    }

    public Transaction getParent() {
        return parent;
    }

    public Long getCommitTimestampDirect() {
        return commitTimestamp;
    }

    public Long getCommitTimestamp() {
        return getCommitTimestamp(getRootTransaction());
    }

    public Long getCommitTimestamp(ImmutableTransaction scope) {
        return behavior.getGlobalCommitTimestamp(scope, globalCommitTimestamp, commitTimestamp, parent);
    }

    @Override
    public String toString() {
        return "Transaction: " + getTransactionId() + " status: " + status;
    }

    public boolean needsGlobalCommitTimestamp() {
        return behavior.needsGlobalCommitTimestamp();
    }

}
