package com.splicemachine.si.impl;

import java.util.Collections;
import java.util.Set;

import static com.splicemachine.si.impl.TransactionStatus.ACTIVE;
import static com.splicemachine.si.impl.TransactionStatus.COMMITTED;
import static com.splicemachine.si.impl.TransactionStatus.COMMITTING;
import static com.splicemachine.si.impl.TransactionStatus.ERROR;
import static com.splicemachine.si.impl.TransactionStatus.LOCAL_COMMIT;
import static com.splicemachine.si.impl.TransactionStatus.ROLLED_BACK;

/**
 * Represents an application level transaction that spans many atomic writes to the underlying data store (i.e. HBase).
 */
public class Transaction extends ImmutableTransaction {
    /**
     * the last time keepAlive was called on this transaction (or when the transaction began). This is an actual timestamp
     * (i.e. number of ms since Jan 1, 1970), not a sequence number.
     */
    public final long keepAlive;

    /**
     * the time when the transaction committed or null if it has not committed
     */
    public final Long commitTimestamp;

    /**
     * all of the direct child transactions of this transaction
     */
    private final Set<Long> children;
    private final TransactionStatus status;
    /**
     * the parent transaction or null if this is a root level transaction
     */
    private final Transaction parent;

    /**
     * indicates that a dependent transaction has performed a local commit, but the parent may not have committed
     */
    private final boolean locallyCommitted;

    public Transaction(long beginTimestamp, long keepAlive, Transaction parent, Set<Long> children,
                       Boolean dependent, boolean allowWrites,
                       Boolean readUncommitted, Boolean readCommitted, TransactionStatus status,
                       Long commitTimestamp) {
        super(dependent, allowWrites, readCommitted, parent, readUncommitted, beginTimestamp);
        this.keepAlive = keepAlive;
        this.commitTimestamp = commitTimestamp;
        this.parent = parent;
        this.children = children;
        // handle the LOCAL_COMMIT status as a separate boolean because it makes it easy to check if the child status is null
        if (LOCAL_COMMIT.equals(status)) {
            this.status = null;
            this.locallyCommitted = true;
        } else {
            this.status = status;
            this.locallyCommitted = false;
        }
    }

    /**
     * Retrieve a set of all of the direct child transactions under this transaction.
     */
    public Set<Long> getChildren() {
        return Collections.unmodifiableSet(children);
    }

    // immediate functions - These functions are based solely on the transaction's immediate state (i.e. not on the
    // state of their parent)

    /**
     * Returns true if this is a nested, dependent transaction that has been locally committed. Meaning the child
     * transaction was committed. This is separate from whether the parent transaction is committed.
     */
    public boolean isLocallyCommitted() {
        return locallyCommitted;
    }

    /**
     * Returns true if this transaction is still running as determined by the fact that it has not entered a
     * terminal state.
     */
    public boolean isActive() {
        return !isFinished();
    }

    private boolean isFinished() {
        if (isNested()) {
            return status != null && inTerminalStatus();
        } else {
            return inTerminalStatus();
        }
    }

    private boolean inTerminalStatus() {
        return statusIsOneOf(ERROR, ROLLED_BACK, COMMITTED);
    }

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
     *         if they don't have one explictly set for themselves.
     */
    public TransactionStatus getEffectiveStatus() {
        if (shouldUseParentStatus()) {
            return parent.getEffectiveStatus();
        }
        return status;
    }

    private boolean shouldUseParentStatus() {
        return status == null;
    }

    /**
     * @return true if this transaction is still active, based on it's parent status.
     */
    public boolean isEffectivelyActive() {
        return getEffectiveStatus().equals(ACTIVE);
    }

    /**
     * @param otherTransaction
     * @return true if this transaction and the otherTransaction are part of the same root transaction.
     */
    public boolean isEffectivelyPartOfTransaction(ImmutableTransaction otherTransaction) {
        return getRootBeginTimestamp() == otherTransaction.getRootBeginTimestamp();
    }

    //

    /**
     * @param otherTransaction
     * @return true if this transaction is finally committed and has committed after the otherTransaction began
     */
    public boolean committedAfter(ImmutableTransaction otherTransaction) {
        return isCommitted() && (commitTimestamp > otherTransaction.getRootBeginTimestamp());
    }

    /**
     * @param otherTransaction
     * @return true if the otherTransaction did not fail and is part of the same root transaction as this transaction
     */
    public boolean isVisiblePartOfTransaction(ImmutableTransaction otherTransaction) {
        return (isCommitted() || isEffectivelyActive())
                && isEffectivelyPartOfTransaction(otherTransaction);
    }

}
