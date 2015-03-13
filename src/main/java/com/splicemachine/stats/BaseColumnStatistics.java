package com.splicemachine.stats;

import java.io.DataOutput;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 3/5/15
 */
public abstract class BaseColumnStatistics<T extends Comparable<T>> implements ColumnStatistics<T> {
    protected int columnId;
    protected long totalBytes;
    protected long totalCount;
    protected long nullCount;
    protected long minCount;

    public BaseColumnStatistics(int columnId, long totalBytes, long totalCount, long nullCount,long minCount) {
        this.columnId = columnId;
        this.totalBytes = totalBytes;
        this.totalCount = totalCount;
        this.nullCount = nullCount;
        this.minCount = minCount;
    }

    @Override public int columnId() { return columnId; }

    @Override
    public int avgColumnWidth() {
        if(totalCount<=0||totalCount==nullCount) return 0;
        return (int)(totalBytes/totalCount);
    }

    @Override public long nonNullCount() { return totalCount-nullCount; }
    @Override public long nullCount() { return nullCount; }
    @Override public long minCount() { return minCount; }

    @Override
    public float nullFraction() {
        if(totalCount<=0) return 0f;
        return ((float)nullCount)/totalCount;
    }

    protected static void write(ColumnStatistics<?> item,DataOutput output) throws IOException {
        assert item instanceof BaseColumnStatistics: "Cannot encode non-BaseColumnStatistics";
        BaseColumnStatistics bsc = (BaseColumnStatistics)item;
        output.writeInt(bsc.columnId);
        output.writeLong(bsc.totalBytes);
        output.writeLong(bsc.totalCount);
        output.writeLong(bsc.nullCount);
        output.writeLong(bsc.minCount);
    }

}
