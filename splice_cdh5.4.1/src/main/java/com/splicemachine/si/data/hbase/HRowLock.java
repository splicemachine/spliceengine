package com.splicemachine.si.data.hbase;

import com.splicemachine.si.api.data.SRowLock;
import org.apache.hadoop.hbase.regionserver.HRegion;

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

    @Override
    public void unlock() {
        delegate.release();
    }
}