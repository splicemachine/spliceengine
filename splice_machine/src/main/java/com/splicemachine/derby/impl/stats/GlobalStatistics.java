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

    public GlobalStatistics(String tableId,List<PartitionStatistics> partitionStatistics) {
        this.partitionStatistics = partitionStatistics;
        this.tableId = tableId;
    }

    @Override
    public long rowCount() {
        long rc = 0;
        for(PartitionStatistics statistics: partitionStatistics)
            rc+=statistics.rowCount();
        return rc;
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
    public double localReadLatency() {
        double rc = 0;
        if(partitionStatistics.size()<=0) return 0l;

        for(PartitionStatistics statistics: partitionStatistics)
            rc+=statistics.localReadLatency();
        return rc /partitionStatistics.size();
    }

    @Override
    public double remoteReadLatency() {
        double rc = 0;
        if(partitionStatistics.size()<=0) return 0l;

        for(PartitionStatistics statistics: partitionStatistics)
            rc+=statistics.remoteReadLatency();
        return rc /partitionStatistics.size();
    }

    @Override
    public long localReadTime() {
        if(partitionStatistics.size()<=0) return 0l;
        long rc = 0l;
        for(PartitionStatistics statistics:partitionStatistics){
           rc+=statistics.localReadTime();
        }
        return rc/partitionStatistics.size();
    }

    @Override
    public long remoteReadTime() {
        if(partitionStatistics.size()<=0) return 0l;
        long rc = 0l;
        for(PartitionStatistics statistics:partitionStatistics){
            rc+=statistics.remoteReadTime();
        }
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
