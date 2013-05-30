package com.splicemachine.si.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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

    public Transaction(TransactionStore transactionStore, long id, long beginTimestamp, long keepAlive, Transaction parent,
                       boolean dependent, Set<Long> children,
                       boolean allowWrites, Boolean readUncommitted, Boolean readCommitted, TransactionStatus status,
                       Long commitTimestamp, Long globalCommitTimestamp, Long counter) {
        super(transactionStore, id, allowWrites, readCommitted, parent, dependent, readUncommitted, beginTimestamp);
        this.keepAlive = keepAlive;
        this.parent = parent;
        this.children = children;
        this.commitTimestamp = commitTimestamp;
        this.globalCommitTimestamp = globalCommitTimestamp;
        this.status = status;
        this.counter = counter;
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
     *         if they don't have one explicitly set for themselves.
     */
    public TransactionStatus getEffectiveStatus() {
        return getEffectiveStatus(Transaction.getRootTransaction());
    }

    public TransactionStatus getEffectiveStatus(ImmutableTransaction effectiveTransaction) {
        if (shouldUseParentStatus()
                && !sameTransaction(effectiveTransaction)
                && !statusIsOneOf(ERROR, ROLLED_BACK)) {
            return parent.getEffectiveStatus(effectiveTransaction);
        }
        return status;
    }

    public TransactionStatus getStatus() {
        return status;
    }

    private boolean shouldUseParentStatus() {
        return ((status.equals(ACTIVE) || status.equals(COMMITTED))
                && !parent.isRootTransaction());
    }

    /**
     * @return true if this transaction is still active, based on it's parent status.
     */
    public boolean isEffectivelyActive() {
        return getEffectiveStatus().equals(ACTIVE);
    }

    ////

    public static Transaction getRootTransaction() {
        return new Transaction(null, -1, 0, 0, null, true, null, false, false, false, TransactionStatus.ACTIVE, null,
                null, null);
    }

    public Transaction getParent() {
        return parent;
    }

    public Long getCommitTimestamp() {
        return commitTimestamp;
    }

    public Long getGlobalCommitTimestamp() {
        if (parent.isRootTransaction()) {
            return commitTimestamp;
        } else if (isIndependent()) {
            return globalCommitTimestamp;
        } else {
            return parent.getGlobalCommitTimestamp();
        }
    }

    public long getCommitTimestamp(ImmutableTransaction scope) {
        if (isIndependent() && scope.isRootTransaction()) {
            return globalCommitTimestamp;
        } else {
            return commitTimestamp;
        }
    }
}
