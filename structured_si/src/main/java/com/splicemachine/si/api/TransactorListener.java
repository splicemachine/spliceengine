package com.splicemachine.si.api;

public interface TransactorListener {
    void beginTransaction(boolean nested);
    void commitTransaction();
    void rollbackTransaction();
    void failTransaction();
    void writeTransaction();
    void loadTransaction();
}
