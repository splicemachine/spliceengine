package com.splicemachine.si.impl;

/**
 * An SI transaction passes through the following states.
 * The successful path is ACTIVE -> COMMITTING -> COMMITTED.
 * Alternate paths are ACTIVE -> ERROR and ACTIVE -> ROLLED_BACK.
 */
public enum TransactionStatus {
    ACTIVE,
    ERROR,
    COMMITTING,
    COMMITTED,
    ROLLED_BACK;

    public boolean isActive() {
        return this.equals(ACTIVE);
    }

    public boolean isFinished() {
        return !this.isActive();
    }

    public boolean isCommitted() {
        return this.equals(COMMITTED);
    }

    public boolean isCommitting() {
        return this.equals(COMMITTING);
    }

}
