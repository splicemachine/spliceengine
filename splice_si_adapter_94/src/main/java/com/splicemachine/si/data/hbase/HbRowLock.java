package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.SRowLock;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * @author Scott Fines
 *         Date: 11/12/14
 */
public class HbRowLock implements SRowLock {
    private final Integer rowLock;
    private final HRegion region;

    public HbRowLock(Integer rowLock, HRegion region) {
        this.rowLock = rowLock;
        this.region = region;
    }

    @Override
    public void unlock() {
        region.releaseRowLock(rowLock);
    }

    @Override
    public Integer getLockId() {
        return rowLock;
    }

}
