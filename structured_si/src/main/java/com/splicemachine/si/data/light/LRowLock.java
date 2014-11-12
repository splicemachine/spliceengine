package com.splicemachine.si.data.light;

import com.splicemachine.si.data.api.SRowLock;

/**
 * @author Scott Fines
 *         Date: 11/12/14
 */
public class LRowLock implements SRowLock {
    private final Integer lockId;
    private final LStore store;
    private final LTable table;

    public LRowLock(Integer lockId, LStore store, LTable table) {
        this.lockId = lockId;
        this.store = store;
        this.table = table;
    }

    @Override
    public void unlock() {
        this.store.unLockRow(table,this);
    }

    @Override
    public Integer getLockId() {
        return lockId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        LRowLock lRowLock = (LRowLock) o;

        return lockId.equals(lRowLock.lockId);
    }

    @Override
    public int hashCode() {
        return lockId.hashCode();
    }

    @Override
    public void unlock() {
       //no-op
    }
}
