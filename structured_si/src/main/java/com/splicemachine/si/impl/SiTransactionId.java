package com.splicemachine.si.impl;

import com.splicemachine.si.api.TransactionId;

public class SiTransactionId implements TransactionId {
    private final long id;
    public final boolean nestedReadOnly;
    private final String NRO = "NRO";

    public SiTransactionId(long id) {
        this.id = id;
        this.nestedReadOnly = false;
    }

    public SiTransactionId(long id, boolean nestedReadOnly) {
        this.id = id;
        this.nestedReadOnly = nestedReadOnly;
    }

    public SiTransactionId(String transactionId) {
        if (transactionId.endsWith(NRO)) {
            this.id = Long.parseLong(transactionId.substring(0, transactionId.length() - NRO.length()));
            this.nestedReadOnly = true;
        } else {
            this.id = Long.parseLong(transactionId);
            this.nestedReadOnly = false;
        }
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public String getTransactionIdString() {
        final String baseId = Long.valueOf(id).toString();
        final String suffix = nestedReadOnly ? NRO : "";
        return baseId + suffix;
    }
}
