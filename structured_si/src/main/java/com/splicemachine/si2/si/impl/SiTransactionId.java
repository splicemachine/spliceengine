package com.splicemachine.si2.si.impl;

import com.splicemachine.si2.si.api.TransactionId;

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
    public String getTransactionID() {
        return Long.valueOf(id).toString();
    }
}
