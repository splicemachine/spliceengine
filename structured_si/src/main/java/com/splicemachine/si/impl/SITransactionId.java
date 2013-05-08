package com.splicemachine.si.impl;

import com.splicemachine.si.api.TransactionId;

public class SiTransactionId implements TransactionId {
    private final long id;
    public final boolean independentReadOnly;
    private final String IRO = ".IRO";

    public SiTransactionId(long id) {
        this.id = id;
        this.independentReadOnly = false;
    }

    public SiTransactionId(long id, boolean independentReadOnly) {
        this.id = id;
        this.independentReadOnly = independentReadOnly;
    }

    public SiTransactionId(String transactionId) {
        if (transactionId.endsWith(IRO)) {
            this.id = Long.parseLong(transactionId.substring(0, transactionId.length() - IRO.length()));
            this.independentReadOnly = true;
        } else {
            this.id = Long.parseLong(transactionId);
            this.independentReadOnly = false;
        }
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public String getTransactionIdString() {
        final String baseId = Long.valueOf(id).toString();
        final String suffix = independentReadOnly ? IRO : "";
        return baseId + suffix;
    }
}
