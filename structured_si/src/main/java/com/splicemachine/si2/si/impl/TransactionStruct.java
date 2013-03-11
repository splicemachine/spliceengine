package com.splicemachine.si2.si.impl;

public class TransactionStruct {
    public final long beginTimestamp;
    public final TransactionStatus status;
    public final Long commitTimestamp;

    public TransactionStruct(long beginTimestamp, TransactionStatus status, Long commitTimestamp) {
        this.beginTimestamp = beginTimestamp;
        this.status = status;
        this.commitTimestamp = commitTimestamp;
    }
}
