package com.splicemachine.derby.iapi.catalog;

import org.apache.derby.iapi.sql.dictionary.TupleDescriptor;

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

    public TableStatisticsDescriptor(long conglomerateId,
                                     String partitionId,
                                     long timestamp,
                                     boolean stale,
                                     boolean inProgress,
                                     long rowCount,
                                     long partitionSize,
                                     int meanRowWidth,
                                     long queryCount) {
        this.conglomerateId = conglomerateId;
        this.partitionId = partitionId;
        this.timestamp = timestamp;
        this.stale = stale;
        this.meanRowWidth = meanRowWidth;
        this.queryCount = queryCount;
        this.partitionSize = partitionSize;
        this.rowCount = rowCount;
        this.inProgress = inProgress;
    }

    public long getConglomerateId() { return conglomerateId; }
    public String getPartitionId() { return partitionId; }
    public long getTimestamp() { return timestamp; }
    public boolean isStale() { return stale; }
    public int getMeanRowWidth() { return meanRowWidth; }
    public long getQueryCount() { return queryCount; }
    public long getPartitionSize() { return partitionSize; }
    public long getRowCount() { return rowCount; }

    public boolean isInProgress() { return inProgress; }
}
