package com.splicemachine.si.api;

public interface TransactorListener {
    void beginTransaction(TransactionId parent);
    void commitTransaction();
    void rollbackTransaction();
    void failTransaction();
    void writeTransaction();
    void loadTransaction();
}
