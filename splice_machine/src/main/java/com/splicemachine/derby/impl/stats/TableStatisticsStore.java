package com.splicemachine.derby.impl.stats;

import com.splicemachine.si.api.TxnView;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 3/9/15
 */
public interface TableStatisticsStore {
    public static class TableStats{
        private long conglomId;
        private String partitionId;
        private long timestamp;
        private long rowCount;
        private long partitionSize;
        private int avgRowWidth;
        private long queryCount;

        public TableStats(long conglomId,
                          String partitionId,
                          long timestamp,
                          long rowCount,
                          long partitionSize,
                          int avgRowWidth,
                          long queryCount) {
            this.conglomId = conglomId;
            this.partitionId = partitionId;
            this.timestamp = timestamp;
            this.rowCount = rowCount;
            this.partitionSize = partitionSize;
            this.avgRowWidth = avgRowWidth;
            this.queryCount = queryCount;
        }

        public long getConglomId() { return conglomId; }
        public String getPartitionId() { return partitionId; }
        public long getTimestamp() { return timestamp; }
        public long getRowCount() { return rowCount; }
        public int getAvgRowWidth() { return avgRowWidth; }
        public long getQueryCount() { return queryCount; }
        public long getPartitionSize() { return partitionSize; }
    }

    public TableStats[] fetchTableStatistics(TxnView txn, long conglomerateId,List<String> partitionsToFetch) throws ExecutionException;
}
