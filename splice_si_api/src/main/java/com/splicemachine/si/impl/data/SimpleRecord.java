package com.splicemachine.si.impl.data;

import com.splicemachine.si.api.data.Record;

/**
 *
 *
 */
public class SimpleRecord implements Record<Object[]> {
    private long txnId1;
    private long txnId2;
    private long version;
    private boolean hasTombstone;
    private long effectiveTimestamp;
    private int numberOfColumns;
    private Object[] data;

    public SimpleRecord() {
    }
    
    @Override
    public long getTxnId1() {
        return txnId1;
    }
    @Override
    public long getTxnId2() {
        return txnId2;
    }

    @Override
    public long getVersion() {
        return version;
    }

    @Override
    public boolean hasTombstone() {
        return hasTombstone;
    }


    @Override
    public long getEffectiveTimestamp() {
        return effectiveTimestamp;
    }

    @Override
    public int numberOfColumns() {
        return numberOfColumns;
    }

    @Override
    public Object[] getData() {
        return data;
    }


    @Override
    public void setTxnId1(long txnId1) {
        this.txnId1 = txnId1;
    }
    @Override
    public void setTxnId2(long txnId2) {
        this.txnId2 = txnId2;
    }

    @Override
    public void setVersion(long version) {
        this.version = version;
    }
    @Override
    public void setHasTombstone(boolean hasTombstone) {
        this.hasTombstone = hasTombstone;
    }

    @Override
    public void setEffectiveTimestamp(long effectiveTimestamp) {
        this.effectiveTimestamp = effectiveTimestamp;
    }

    @Override
    public void setNumberOfColumns(int numberOfColumns) {
        this.numberOfColumns = numberOfColumns;
    }

    @Override
    public void setData(Object[] data) {
        this.data = data;
    }
}
