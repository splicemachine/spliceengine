package com.splicemachine.si2.si.api;

import com.splicemachine.constants.ITransactionState;

/**
 * Opaque object issued to identify a transaction. To be submitted back on future calls.
 */
public interface TransactionId extends ITransactionState {
    long getId();
}
