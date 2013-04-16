package com.splicemachine.si.impl;

import java.util.Set;

public class TransactionStruct extends ImmutableTransactionStruct {
    public final Set<Long> children;
    public final TransactionStatus status;
    public final Long commitTimestamp;
    public final TransactionStruct parent;

    public TransactionStruct(long beginTimestamp, TransactionStruct parent, Set<Long> children,
                             Boolean dependent, boolean allowWrites,
                             Boolean readUncommitted, Boolean readCommitted, TransactionStatus status,
                             Long commitTimestamp) {
        super(dependent, allowWrites, readCommitted, parent, readUncommitted, beginTimestamp);
        this.children = children;
        this.status = status;
        this.commitTimestamp = commitTimestamp;
        this.parent = parent;
    }

    public TransactionStatus getEffectiveStatus() {
        if (shouldUseParentStatus()) {
            return parent.getEffectiveStatus();
        }
        return status;
    }

    private boolean shouldUseParentStatus() {
        return status == null || (parent != null && status != null && status.equals(TransactionStatus.COMMITED) && dependent);
    }

    public boolean canBeRolledBack() {
        return getEffectiveStatus().equals(TransactionStatus.ACTIVE) &&
                (status == null || (status != null && !status.equals(TransactionStatus.COMMITED)));
    }

    public boolean isStillRunning() {
        return !(status != null && (status.equals(TransactionStatus.ERROR) || status.equals(TransactionStatus.ROLLED_BACK) ||
                (status.equals(TransactionStatus.COMMITED) && commitTimestamp != null)));
    }

    public boolean isCacheable() {
        return !isStillRunning();
    }

    public boolean didNotCommit() {
        return commitTimestamp == null;
    }
}
