package com.splicemachine.si.impl;

import com.splicemachine.si.api.TransactionId;

/**
 * Represents an SI transaction identifier. Exposes it as either a long or a string.
 */
public class SITransactionId implements TransactionId {
    private static final String IRO = ".IRO";

    private final long id;
    public final boolean independentReadOnly;

    public SITransactionId(long id) {
        this.id = id;
        this.independentReadOnly = false;
    }

    public SITransactionId(long id, boolean independentReadOnly) {
        this.id = id;
        this.independentReadOnly = independentReadOnly;
    }

    public SITransactionId(String transactionId) {
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

    public boolean isRootTransaction() {
        return getId() == Transaction.getRootTransaction().getTransactionId().getId();
    }

    @Override
    public String toString() {
        return getTransactionIdString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        SITransactionId that = (SITransactionId) o;

        if (id != that.id) return false;
        if (independentReadOnly != that.independentReadOnly) return false;

        return true;
    }

    @Override
    public int hashCode() {
        int result = (int) (id ^ (id >>> 32));
        result = 31 * result + (independentReadOnly ? 1 : 0);
        return result;
    }
}
