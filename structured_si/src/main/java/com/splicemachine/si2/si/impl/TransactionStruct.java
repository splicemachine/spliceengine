package com.splicemachine.si2.si.impl;

public class TransactionStruct {
    final long beginTimestamp;
    final TransactionStatus status;
    final Long commitTimestamp;

    public TransactionStruct(long beginTimestamp, TransactionStatus status, Long commitTimestamp) {
        this.beginTimestamp = beginTimestamp;
        this.status = status;
        this.commitTimestamp = commitTimestamp;
    }
}
