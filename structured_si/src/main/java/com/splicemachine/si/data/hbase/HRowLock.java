package com.splicemachine.si.data.hbase;

import org.apache.hadoop.hbase.regionserver.HRegion;

import com.splicemachine.si.data.api.SRowLock;

/**
 * @author Jeff Cunningham
 *         Date: 3/16/14
 */
public class HRowLock implements SRowLock {
    private final HRegion.RowLock delegate;

    public HRowLock(HRegion.RowLock lock) {
        this.delegate = lock;
    }

    @Override
    public HRegion.RowLock getDelegate() {
        return this.delegate;
    }
}
