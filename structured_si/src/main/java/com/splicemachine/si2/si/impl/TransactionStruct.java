package com.splicemachine.si2.si.impl;

public class TransactionStruct {
    public final long beginTimestamp;
    public final boolean allowWrites;
    public final boolean readUncommitted;
    public final boolean readCommitted;
    public final TransactionStatus status;
    public final Long commitTimestamp;

    public TransactionStruct(long beginTimestamp, boolean allowWrites, boolean readUncommitted, boolean readCommitted,
                             TransactionStatus status, Long commitTimestamp) {
        this.beginTimestamp = beginTimestamp;
        this.allowWrites = allowWrites;
        this.readUncommitted = readUncommitted;
        this.readCommitted = readCommitted;
        this.status = status;
        this.commitTimestamp = commitTimestamp;
    }
}
