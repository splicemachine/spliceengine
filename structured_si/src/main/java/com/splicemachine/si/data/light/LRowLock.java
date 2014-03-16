package com.splicemachine.si.data.light;

import org.apache.hadoop.hbase.regionserver.HRegion;

import com.splicemachine.si.data.api.SRowLock;

/**
 * @author Jeff Cunningham
 *         Date: 3/16/14
 */
public class LRowLock implements SRowLock {
    private final int lockId;

    public LRowLock(int lockId) {
        this.lockId = lockId;
    }

    @Override
    public HRegion.RowLock getDelegate() {
        return null;
    }
}
