package com.splicemachine.si.impl;

public class TransactionRelativeState {
    /**
     * the time when the transaction committed or null if it has not committed
     */
    public final Long commitTimestamp;
    public final TransactionStatus status;

    public TransactionRelativeState(Long commitTimestamp, TransactionStatus status) {
        this.commitTimestamp = commitTimestamp;
        this.status = status;
    }
}
