package com.splicemachine.si.data.api;

public interface SRowLock {

    void unlock();
    Integer getLockId();
}
