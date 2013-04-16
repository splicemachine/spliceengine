package com.splicemachine.si.impl;

import java.util.Collections;
import java.util.Set;

public class Transaction extends ImmutableTransaction {
    public final Long commitTimestamp;

    private final Set<Long> children;
    private final TransactionStatus status;
    private final Transaction parent;
    private final boolean locallyCommitted;

    public Transaction(long beginTimestamp, Transaction parent, Set<Long> children,
                       Boolean dependent, boolean allowWrites,
                       Boolean readUncommitted, Boolean readCommitted, TransactionStatus status,
                       Long commitTimestamp) {
        super(dependent, allowWrites, readCommitted, parent, readUncommitted, beginTimestamp);
        this.children = children;
        this.status = (status == null || status.equals(TransactionStatus.LOCAL_COMMIT)) ? null : status;
        this.locallyCommitted = (status != null && status.equals(TransactionStatus.LOCAL_COMMIT));
        this.commitTimestamp = commitTimestamp;
        this.parent = parent;
    }

    public Set<Long> getChildren() {
        return Collections.unmodifiableSet(children);
    }

    public TransactionStatus getEffectiveStatus() {
        if (shouldUseParentStatus()) {
            return parent.getEffectiveStatus();
        }
        return status;
    }

    private boolean shouldUseParentStatus() {
        return status == null;
    }

    private boolean isNested() {
        return parent != null;
    }

    public boolean isNestedDependent() {
        return isNested() && dependent;
    }

    public boolean isLocallyCommitted() {
        return locallyCommitted;
    }

    // immediate functions

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
        return (status.equals(TransactionStatus.ERROR)
                || status.equals(TransactionStatus.ROLLED_BACK)
                || status.equals(TransactionStatus.COMMITTED));
    }

    public boolean isCommitted() {
        return commitTimestamp != null;
    }

    public boolean isFailed() {
        return status != null &&
                (status.equals(TransactionStatus.ERROR) || status.equals(TransactionStatus.ROLLED_BACK));
    }

    public boolean isCommitting() {
        return status != null && status.equals(TransactionStatus.COMMITTING);
    }

    // effective functions

    public boolean isEffectivelyActive() {
        return getEffectiveStatus().equals(TransactionStatus.ACTIVE);
    }

    public boolean isEffectivelyPartOfTransaction(ImmutableTransaction otherTransaction) {
        return getRootBeginTimestamp() == otherTransaction.getRootBeginTimestamp();
    }

    ////

    public boolean committedAfter(ImmutableTransaction otherTransaction) {
        return isCommitted() && (commitTimestamp > otherTransaction.getRootBeginTimestamp());
    }

    public boolean isCommittedBefore(ImmutableTransaction otherTransaction) {
        return isCommitted() && commitTimestamp < otherTransaction.getRootBeginTimestamp();
    }

    public boolean isVisiblePartOfTransaction(ImmutableTransaction otherTransaction) {
        return (isCommitted() || isEffectivelyActive())
                && isEffectivelyPartOfTransaction(otherTransaction);
    }

}
