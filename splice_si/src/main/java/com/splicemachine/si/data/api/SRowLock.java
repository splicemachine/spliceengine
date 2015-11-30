package com.splicemachine.si.data.api;

import org.apache.hadoop.hbase.regionserver.HRegion;

public interface SRowLock {
    void unlock();
    HRegion.RowLock getDelegate();
}
