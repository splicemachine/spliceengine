package com.splicemachine.si2.si.impl;

public class TransactionStruct {
    public final long beginTimestamp;
    public final boolean allowWrites;
    public final TransactionStatus status;
    public final Long commitTimestamp;

    public TransactionStruct(long beginTimestamp, boolean allowWrites, TransactionStatus status, Long commitTimestamp) {
        this.beginTimestamp = beginTimestamp;
        this.allowWrites = allowWrites;
        this.status = status;
        this.commitTimestamp = commitTimestamp;
    }
}
