package com.splicemachine.si.impl;

import com.splicemachine.si.api.TransactionId;

public class SiTransactionId implements TransactionId {
    private final long id;

    public SiTransactionId(long id) {
        this.id = id;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public String getTransactionIdString() {
        return Long.valueOf(id).toString();
    }
}
