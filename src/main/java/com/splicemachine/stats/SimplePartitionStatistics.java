package com.splicemachine.stats;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 2/24/15
 */
public class SimplePartitionStatistics implements PartitionStatistics {
    private String tableId;
    private String partitionId;
    private long rowCount;
    private long totalBytes;
    private long queryCount;
    private long totalLocalReadTime;
    private long totalRemoteReadLatency;

    private List<ColumnStatistics> columnStatistics;

    @Override public String tableId() { return tableId; }
    @Override public String partitionId() { return partitionId; }
    @Override public long rowCount() { return rowCount; }
    @Override public long totalSize() { return totalBytes; }
    @Override public int avgRowWidth() { return (int)(totalBytes/rowCount); }
    @Override public long queryCount() { return queryCount; }
    @Override public long localReadLatency() { return totalLocalReadTime /rowCount; }
    @Override public long remoteReadLatency() { return totalRemoteReadLatency/rowCount; }
    @Override public long collectionTime() { return totalLocalReadTime; }
    @Override public List<ColumnStatistics> columnStatistics() { return columnStatistics; }

    @Override
    public PartitionStatistics merge(PartitionStatistics other) {
        assert tableId.equals(other.tableId()): "Cannot merge Statistics from two different tables";

        this.rowCount+=other.rowCount();
        this.totalBytes +=other.totalSize();
        this.queryCount+=other.queryCount();

        this.totalLocalReadTime += other.collectionTime();
        this.totalRemoteReadLatency += other.remoteReadLatency()*other.rowCount();

        /*
         * Merge Column Statistics together.
         *
         * Since we are combining partitions from the same table, we should have the same columns,
         * in the same column order. Thus, we just iterate in order
         */
        List<ColumnStatistics> mergedStats = new ArrayList<>(columnStatistics.size());
        List<ColumnStatistics> otherStats = other.columnStatistics();
        for(int i=0;i<columnStatistics.size();i++){
            ColumnStatistics left = columnStatistics.get(i);
            ColumnStatistics right = otherStats.get(i);
            @SuppressWarnings("unchecked") ColumnStatistics merged = (ColumnStatistics) left.merge(right);
            mergedStats.add(merged);
        }
        this.columnStatistics = mergedStats;

        return this;
    }
}
