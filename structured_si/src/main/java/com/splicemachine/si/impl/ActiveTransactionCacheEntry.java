package com.splicemachine.si.impl;

/**
 * Used as a value in a transaction cache. It stores both the transaction and the time when the transaction was cached.
 * This way the calling code can determine whether the result is recent enough to use.
 */
public class ActiveTransactionCacheEntry {
    public final long effectiveTimestamp;
    public final Transaction transaction;

    public ActiveTransactionCacheEntry(long effectiveTimestamp, Transaction transaction) {
        this.effectiveTimestamp = effectiveTimestamp;
        this.transaction = transaction;
    }
}
