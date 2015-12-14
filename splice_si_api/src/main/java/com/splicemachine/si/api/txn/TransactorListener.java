package com.splicemachine.si.api.txn;

/**
 * Callbacks made during the transaction life-cycle.
 */
public interface TransactorListener {
    void beginTransaction(boolean nested);
    void commitTransaction();
    void rollbackTransaction();
    void failTransaction();
    void writeTransaction();
    void loadTransaction();
}
