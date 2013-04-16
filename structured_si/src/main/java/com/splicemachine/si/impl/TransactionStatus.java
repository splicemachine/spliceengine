package com.splicemachine.si.impl;

public enum TransactionStatus {
    ACTIVE,
    ERROR,
    COMMITTING,
    COMMITTED,
    LOCAL_COMMIT,
    ROLLED_BACK
}
