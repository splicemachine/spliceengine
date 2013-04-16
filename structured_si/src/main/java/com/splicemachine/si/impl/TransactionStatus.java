package com.splicemachine.si.impl;

public enum TransactionStatus {
    ACTIVE,
    ERROR,
    COMMITTING,
    COMMITED,
    ROLLED_BACK
}
