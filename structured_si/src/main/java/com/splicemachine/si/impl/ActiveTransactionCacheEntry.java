package com.splicemachine.si.impl;

public class ActiveTransactionCacheEntry {
    public final long effectiveTimestamp;
    public final Transaction transaction;

    public ActiveTransactionCacheEntry(long effectiveTimestamp, Transaction transaction) {
        this.effectiveTimestamp = effectiveTimestamp;
        this.transaction = transaction;
    }
}
