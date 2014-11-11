package com.splicemachine.si.data.hbase;

import com.splicemachine.si.data.api.SRowLock;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 11/12/14
 */
public class HTableRowLock implements SRowLock {
    private final Integer lockId;
    private final HbTable table;

    public HTableRowLock(Integer lockId, HbTable table) {
        this.lockId = lockId;
        this.table = table;
    }

    @Override
    public void unlock() {
        try {
            table.unLockRow(lockId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Integer getLockId() {
        return lockId;
    }
}
