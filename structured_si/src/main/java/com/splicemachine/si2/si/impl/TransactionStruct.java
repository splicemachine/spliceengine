package com.splicemachine.si2.si.impl;

public class TransactionStruct {
    final long beingTimestamp;
    final TransactionStatus status;
    final Long commitTimestamp;

    public TransactionStruct(long beginTimestamp, TransactionStatus status, Long commitTimestamp) {
        this.beingTimestamp = beginTimestamp;
        this.status = status;
        this.commitTimestamp = commitTimestamp;
    }
}
