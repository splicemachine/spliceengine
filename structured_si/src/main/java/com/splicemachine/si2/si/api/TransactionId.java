package com.splicemachine.si2.si.api;

/**
 * Opaque object issued to identify a transaction. To be submitted back on future calls.
 */
public interface TransactionId {
    long getId();
}
