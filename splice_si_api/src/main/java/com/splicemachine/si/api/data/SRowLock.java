package com.splicemachine.si.api.data;

public interface SRowLock<RowLock> {
    void unlock();
    RowLock getDelegate();
}
