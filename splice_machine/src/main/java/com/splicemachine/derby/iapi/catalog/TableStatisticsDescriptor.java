package com.splicemachine.derby.iapi.catalog;


import com.splicemachine.db.iapi.sql.dictionary.TupleDescriptor;

/**
 * @author Scott Fines
 *         Date: 2/25/15
 */
public class TableStatisticsDescriptor extends TupleDescriptor {
    private long conglomerateId;
    private String partitionId;
    private long timestamp;
    private boolean stale;
    private int meanRowWidth;
    private long queryCount;
    private long partitionSize;
    private long rowCount;
    private boolean inProgress;
    private long totalLocalReadLatency;
    private long totalRemoteReadLatency;
    private long totalWriteLatency;
    private long openScannerLatency;
    private long closeScannerLatency;

    public TableStatisticsDescriptor(long conglomerateId,
                                     String partitionId,
                                     long timestamp,
                                     boolean stale,
                                     boolean inProgress,
                                     long rowCount,
                                     long partitionSize,
                                     int meanRowWidth,
                                     long queryCount,
                                     long totalLocalReadLatency,
                                     long totalRemoteReadLatency,
                                     long totalWriteLatency,
                                     long openScannerLatency,
                                     long closeScannerLatency) {
        this.conglomerateId = conglomerateId;
        this.partitionId = partitionId;
        this.timestamp = timestamp;
        this.stale = stale;
        this.meanRowWidth = meanRowWidth;
        this.queryCount = queryCount;
        this.partitionSize = partitionSize;
        this.rowCount = rowCount;
        this.inProgress = inProgress;
        this.totalLocalReadLatency = totalLocalReadLatency;
        this.totalRemoteReadLatency = totalRemoteReadLatency;
        this.totalWriteLatency = totalWriteLatency;
        this.openScannerLatency = openScannerLatency;
        this.closeScannerLatency = closeScannerLatency;
    }

    public long getConglomerateId() { return conglomerateId; }
    public String getPartitionId() { return partitionId; }
    public long getTimestamp() { return timestamp; }
    public boolean isStale() { return stale; }
    public int getMeanRowWidth() { return meanRowWidth; }
    public long getQueryCount() { return queryCount; }
    public long getPartitionSize() { return partitionSize; }
    public long getRowCount() { return rowCount; }
    public long getLocalReadLatency() {  return totalLocalReadLatency; }
    public long getRemoteReadLatency() { return totalRemoteReadLatency; }
    public long getWriteLatency() { return totalWriteLatency; }
    public boolean isInProgress() { return inProgress; }
    public long getOpenScannerLatency(){ return openScannerLatency; }
    public long getCloseScannerLatency(){ return closeScannerLatency; }
}
