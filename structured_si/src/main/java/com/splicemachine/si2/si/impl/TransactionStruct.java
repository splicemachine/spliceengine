package com.splicemachine.si2.si.impl;

public class TransactionStruct {
    public final long beginTimestamp;
    public final TransactionStruct parent;
    public final Boolean dependent;
    public final boolean allowWrites;
    public final Boolean readUncommitted;
    public final Boolean readCommitted;
    public final TransactionStatus status;
    public final Long commitTimestamp;

    public TransactionStruct(long beginTimestamp, TransactionStruct parent, Boolean dependent, boolean allowWrites,
                             Boolean readUncommitted, Boolean readCommitted, TransactionStatus status,
                             Long commitTimestamp) {
        this.beginTimestamp = beginTimestamp;
        this.parent = parent;
        this.dependent = dependent;
        this.allowWrites = allowWrites;
        this.readUncommitted = readUncommitted;
        this.readCommitted = readCommitted;
        this.status = status;
        this.commitTimestamp = commitTimestamp;
    }

    public TransactionStatus getEffectiveStatus() {
        if (status == null) {
            return parent.getEffectiveStatus();
        }
        return status;
    }

    public boolean getEffectiveReadUncommitted() {
        if (readUncommitted == null) {
            return parent.getEffectiveReadUncommitted();
        }
        return readUncommitted;
    }

    public boolean getEffectiveReadCommitted() {
        if (readCommitted == null) {
            return parent.getEffectiveReadCommitted();
        }
        return readCommitted;
    }

    public long getRootBeginTimestamp() {
        if (parent == null) {
            return beginTimestamp;
        }
        return parent.getRootBeginTimestamp();
    }
}
