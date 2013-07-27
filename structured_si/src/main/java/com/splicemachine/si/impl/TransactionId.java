package com.splicemachine.si.impl;

/**
 * Represents an SI transaction identifier. Exposes it as either a long or a string.
 */
public class TransactionId {
    private static final String IRO = ".IRO";

    private final long id;
    public final boolean independentReadOnly;

    TransactionId(long id) {
        this.id = id;
        this.independentReadOnly = false;
    }

    TransactionId(long id, boolean independentReadOnly) {
        this.id = id;
        this.independentReadOnly = independentReadOnly;
    }

    public TransactionId(String transactionId) {
        if (transactionId.endsWith(IRO)) {
            this.id = Long.parseLong(transactionId.substring(0, transactionId.length() - IRO.length()));
            this.independentReadOnly = true;
        } else {
            this.id = Long.parseLong(transactionId);
            this.independentReadOnly = false;
        }
    }

    public long getId() {
        return id;
    }

    public String getTransactionIdString() {
        final String baseId = Long.valueOf(id).toString();
        final String suffix = independentReadOnly ? IRO : "";
        return baseId + suffix;
    }

    boolean isRootTransaction() {
        return getId() == Transaction.rootTransaction.getTransactionId().getId();
    }

    @Override
    public String toString() {
        return getTransactionIdString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        TransactionId that = (TransactionId) o;

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
