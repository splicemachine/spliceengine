package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.PartitionStatistics;
import com.splicemachine.stats.estimate.Distribution;
import com.splicemachine.stats.estimate.GlobalDistribution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 3/9/15
 */
public class GlobalStatistics implements OverheadManagedTableStatistics {
    private final String tableId;
    private final List<OverheadManagedPartitionStatistics> partitionStatistics;

    private transient List<ColumnStatistics> combinedColumnStatistics;

    public GlobalStatistics(String tableId,List<OverheadManagedPartitionStatistics> partitionStatistics) {
        this.partitionStatistics = partitionStatistics;
        this.tableId = tableId;
    }

    @Override
    public double openScannerLatency(){
        double tot = 0d;
        long numEvents = 0l;
        for(OverheadManagedPartitionStatistics pStats:partitionStatistics){
            tot+=pStats.getOpenScannerLatency();
            numEvents+=pStats.numOpenEvents();
        }
        if(numEvents<=0l) return 0d;
        return tot/numEvents;
    }

    @Override
    public double closeScannerLatency(){
        double tot = 0d;
        long numEvents = 0l;
        for(OverheadManagedPartitionStatistics pStats:partitionStatistics){
            tot+=pStats.getCloseScannerLatency();
            numEvents+=pStats.numCloseEvents();
        }
        if(numEvents<=0l) return 0d;
        return tot/numEvents;
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
        if(combinedColumnStatistics==null){
            combinedColumnStatistics = mergeColumnStats(partitionStatistics);
        }
        return combinedColumnStatistics;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> ColumnStatistics<T> columnStatistics(int columnId){
        List<ColumnStatistics> colStats = columnStatistics();
        for(ColumnStatistics cStats:colStats){
            if(cStats.columnId()==columnId) return (ColumnStatistics<T>)cStats;
        }
        return null;
    }

    @Override
    public <T> Distribution<T> columnDistribution(int columnId) {
        return new GlobalDistribution<>(this,columnId);
    }

    @Override
    public List<? extends PartitionStatistics> partitionStatistics() {
        return partitionStatistics;
    }

    @Override
    public PartitionStatistics merge(PartitionStatistics other) {
        throw new UnsupportedOperationException("Cannot merge Global statistics!");
    }

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private List<ColumnStatistics> mergeColumnStats(List<OverheadManagedPartitionStatistics> partitionStatistics){
        if(partitionStatistics.size()<=0) return Collections.emptyList();
        List<ColumnStatistics> stats = null;
        for(OverheadManagedPartitionStatistics partStats:partitionStatistics){
            List<ColumnStatistics> colStats=partStats.columnStatistics();
            if(colStats.size()<=0) continue;
            if(stats==null)
                stats = new ArrayList<>(colStats.size());
            for(ColumnStatistics cStats:colStats){
                boolean found = false;
                for(ColumnStatistics existing:stats){
                    if(existing.columnId()==cStats.columnId()){
                        //noinspection unchecked
                        existing.merge(cStats);
                        found=true;
                        break;
                    }
                }
                if(!found){
                    ColumnStatistics cAvg = cStats.getClone();
                    stats.add(cAvg);
                }
            }
        }
        return stats;
    }
}
