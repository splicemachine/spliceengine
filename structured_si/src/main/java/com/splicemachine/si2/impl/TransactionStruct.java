package com.splicemachine.si2.impl;

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
        if (status == null || (parent != null && status != null && status.equals(TransactionStatus.COMMITED) && dependent)) {
            return parent.getEffectiveStatus();
        }
        return status;
    }

    public boolean isCacheable() {
        return (status != null && (status.equals(TransactionStatus.ERROR) || status.equals(TransactionStatus.ABORT) ||
                (status.equals(TransactionStatus.COMMITED) && commitTimestamp != null)));
    }
}
