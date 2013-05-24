package com.splicemachine.si.impl;

/**
 * An SI transaction passes through the following states. The successful path is ACTIVE -> COMMITTING -> COMMITTED.
 * Alternate paths are ACTIVE -> ERROR and ACTIVE -> ROLLED_BACK. The LOCAL_COMMIT state is used for nested transactions.
 */
public enum TransactionStatus {
    ACTIVE,
    ERROR,
    COMMITTING,
    COMMITTED,
    ROLLED_BACK
}
