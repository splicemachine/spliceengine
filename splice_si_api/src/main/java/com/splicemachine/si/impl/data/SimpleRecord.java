package com.splicemachine.si.impl.data;

import com.splicemachine.si.api.data.Record;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;

/**
 *
 *
 */
public class SimpleRecord implements Record<UnsafeRow> {
    private long transactionId1;
    private long transactionId2;
    private long version;
    private boolean hasTombstone;
    private long effectiveTimestamp;
    private int numberOfColumns;
    private UnsafeRow data;

    
    @Override
    public long getTxnId1() {
        return transactionId1;
    }
    @Override
    public long getTxnId2() {
        return transactionId2;
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
    public UnsafeRow getData() {
        return data;
    }


    @Override
    public void setTxnId1(long transactionId1) {
        this.transactionId1 = transactionId1;
    }
    @Override
    public void setTxnId2(long transactionId2) {
        this.transactionId2 = transactionId2;
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
    public void setData(UnsafeRow data) {
        this.data = data;
    }
}
