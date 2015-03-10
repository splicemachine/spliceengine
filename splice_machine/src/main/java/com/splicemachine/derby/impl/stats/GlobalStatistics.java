package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.PartitionStatistics;
import com.splicemachine.stats.TableStatistics;
import com.splicemachine.stats.estimate.Distribution;

import java.util.List;

/**
 * @author Scott Fines
 *         Date: 3/9/15
 */
public class GlobalStatistics implements TableStatistics {
    private final String tableId;
    private final List<PartitionStatistics> partitionStatistics;

    /*
     * We cache fields here, so that multiple requests won't have to go through a potentially expensive
     * rebuilding process here whenever our data is necessary. This makes the assumption that the Partition
     * Statistics don't change
     */
    private transient long cachedRowCount = -1;
    private transient long cachedSize = -1;

    public GlobalStatistics(String tableId,List<PartitionStatistics> partitionStatistics) {
        this.partitionStatistics = partitionStatistics;
        this.tableId = tableId;
    }

    @Override
    public long rowCount() {
        if(cachedRowCount<0){
            long rc = 0;
            for(PartitionStatistics statistics: partitionStatistics)
                rc+=statistics.rowCount();
            cachedRowCount = rc;
        }
        return cachedRowCount;
    }

    @Override
    public long totalSize() {
        long rc = 0;
        for(PartitionStatistics statistics: partitionStatistics)
            rc+=statistics.totalSize();
        return rc;
    }

    @Override
    public long avgSize() {
        long rc = 0;
        if(partitionStatistics.size()<=0) return 0l;

        for(PartitionStatistics statistics: partitionStatistics)
            rc+=statistics.totalSize();
        return rc/partitionStatistics.size();
    }

    @Override
    public long localReadLatency() {
        long rc = 0;
        if(partitionStatistics.size()<=0) return 0l;

        for(PartitionStatistics statistics: partitionStatistics)
            rc+=statistics.localReadLatency();
        return rc/partitionStatistics.size();
    }

    @Override
    public long remoteReadLatency() {
        long rc = 0;
        if(partitionStatistics.size()<=0) return 0l;

        for(PartitionStatistics statistics: partitionStatistics)
            rc+=statistics.remoteReadLatency();
        return rc/partitionStatistics.size();
    }

    @Override
    public int avgRowWidth() {
        int rc = 0;
        if(partitionStatistics.size()<=0) return 0;

        for(PartitionStatistics statistics: partitionStatistics)
            rc+=statistics.avgRowWidth();
        return rc/partitionStatistics.size();
    }

    @Override
    public long queryCount() {
        long rc = 0;
        if(partitionStatistics.size()<=0) return 0l;

        for(PartitionStatistics statistics: partitionStatistics)
            rc+=statistics.queryCount();
        return rc/partitionStatistics.size();
    }

    @Override public String partitionId() { return tableId(); }
    @Override public String tableId() { return tableId; }

    @Override
    public long collectionTime() {
        long maxColTime = 0l;
        for(PartitionStatistics stat:this.partitionStatistics){
            if(maxColTime<stat.collectionTime())
                maxColTime = stat.collectionTime();
        }
        return maxColTime;
    }

    @Override
    public List<ColumnStatistics> columnStatistics() {
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public <T> Distribution<T> columnDistribution(int columnId) {
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public List<PartitionStatistics> partitionStatistics() {
        return partitionStatistics;
    }

    @Override
    public PartitionStatistics merge(PartitionStatistics other) {
        throw new UnsupportedOperationException("Cannot merge Global statistics!");
    }
}
