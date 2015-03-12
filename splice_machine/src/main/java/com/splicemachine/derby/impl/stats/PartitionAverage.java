package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.PartitionStatistics;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.estimate.EmptyDistribution;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 3/9/15
 */
public class PartitionAverage implements PartitionStatistics {
    private final String tableId;
    private final String partitionId;
    private long totalRowCount;
    private int mergeCount;
    private long totalSize;
    private long totalQueryCount;
    private double totalLocalReadLatency;
    private double totalRemoteReadLatency;
    private long totalLocalReadTime;
    private long totalRemoteReadTime;
    private long totalCollectionTime;

    private List<ColumnStatistics> columnStats;

    public PartitionAverage(String tableId, String partitionId) {
        this.tableId = tableId;
        this.partitionId = partitionId;
    }

    public PartitionAverage copy(String newPartition){
        PartitionAverage pa = new PartitionAverage(tableId,newPartition);
        pa.totalRowCount = totalRowCount;
        pa.mergeCount = mergeCount;
        pa.totalSize = totalSize;
        pa.totalQueryCount = totalQueryCount;
        pa.totalLocalReadLatency = totalLocalReadLatency;
        pa.totalRemoteReadLatency = totalRemoteReadLatency;
        pa.totalLocalReadTime = totalLocalReadTime;
        pa.totalRemoteReadTime = totalRemoteReadTime;
        pa.totalCollectionTime = totalCollectionTime;
        return pa;
    }

    @Override
    public long rowCount() {
        if(mergeCount<=0) return 0l;
        return totalRowCount/mergeCount;
    }

    @Override
    public long totalSize() {
        if(mergeCount<=0) return 0l;
        return totalSize/mergeCount;
    }

    @Override
    public int avgRowWidth() {
        if(mergeCount<=0) return 0;
        if(totalRowCount<=0) return 0;
        return (int)(totalSize/totalRowCount);
    }

    @Override
    public long queryCount() {
        if(mergeCount<=0) return 0;
        return totalQueryCount/mergeCount;
    }

    @Override public String partitionId() { return partitionId; }
    @Override public String tableId() { return tableId; }

    @Override
    public double localReadLatency() {
        if(mergeCount<=0) return 0l;
        return totalLocalReadLatency /mergeCount;
    }

    @Override
    public double remoteReadLatency() {
        if(mergeCount<=0) return 0l;
        return totalRemoteReadLatency /mergeCount;
    }

    @Override
    public long localReadTime() {
        if(mergeCount<=0)
            return 0l;
        return totalLocalReadTime/mergeCount;
    }

    @Override
    public long remoteReadTime() {
        if(mergeCount<=0)
            return 0l;
        return totalRemoteReadTime/mergeCount;
    }

    @Override
    public long collectionTime() {
        if(mergeCount<=0) return 0;
        return totalCollectionTime/mergeCount;
    }

    @Override
    public List<ColumnStatistics> columnStatistics() {
        return columnStats;
    }

    @Override
    public <T> Distribution<T> columnDistribution(int columnId) {
        for(ColumnStatistics stats:columnStats){
            if(stats.columnId()==columnId) //noinspection unchecked
                return stats.getDistribution();
        }
        return EmptyDistribution.emptyDistribution();
    }

    @Override
    public <T> ColumnStatistics<T> columnStatistics(int columnId){
        for(ColumnStatistics stats:columnStats){
            if(stats.columnId()==columnId) return stats;
        }
        return null;
    }

    @Override
    public PartitionStatistics merge(PartitionStatistics other) {
        totalRowCount+=other.rowCount();
        totalSize+=other.totalSize();
        totalQueryCount+=other.queryCount();
        totalLocalReadLatency+=other.localReadLatency();
        totalRemoteReadLatency+=other.remoteReadLatency();
        totalLocalReadTime+=other.localReadTime();
        totalRemoteReadTime+=other.remoteReadTime();
        totalCollectionTime+=other.collectionTime();
        mergeColumns(other.columnStatistics());
        mergeCount++;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PartitionAverage)) return false;

        PartitionAverage that = (PartitionAverage) o;

        return partitionId.equals(that.partitionId) && tableId.equals(that.tableId);
    }

    @Override
    public int hashCode() {
        int result = tableId.hashCode();
        result = 31 * result + partitionId.hashCode();
        return result;
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void mergeColumns(List<ColumnStatistics> columnStatisticses) {
        if(columnStats==null){
            columnStats = new ArrayList<>(columnStatisticses.size());
        }
        for(int i=0;i<columnStatisticses.size();i++){
            ColumnStatistics toMerge = columnStatisticses.get(i);
            boolean found = false;
            for(int j=0;j<columnStats.size();j++){
                ColumnStatistics myStats = columnStats.get(j);
                if(myStats.columnId()==toMerge.columnId()) {
                    myStats.merge(toMerge);
                    //we don't have to reset the list element, because we know myStats implementation is mutable
                    found =true;
                    break;
                }
            }
            if(!found){
                columnStats.add(ColumnAverage.fromExisting(toMerge));
            }
        }
    }
}
