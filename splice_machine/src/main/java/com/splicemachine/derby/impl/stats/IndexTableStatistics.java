package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.PartitionStatistics;
import com.splicemachine.stats.estimate.Distribution;

import java.util.ArrayList;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 5/5/15
 */
public class IndexTableStatistics implements OverheadManagedTableStatistics{
    private final OverheadManagedTableStatistics indexStats;
    private final OverheadManagedTableStatistics baseTableStats;

    private final double indexSizeFactor;
    private final int baseOverhead;
    private final double indexOverhead;

    private transient List<OverheadManagedPartitionStatistics> indexPartStats;

    /**
     * @param indexStats Estimated index stats, we don't collect index statistics anymore
     * @param baseTableStats Statistics for the base table, we derive index stats from these
     * @param columnFraction Fraction of columns in the index vs the base table
     * @param baseOverhead
     * @param indexOverhead
     */
    public IndexTableStatistics(OverheadManagedTableStatistics indexStats,
                                OverheadManagedTableStatistics baseTableStats,
                                double columnFraction, int baseOverhead, double indexOverhead){
        this.indexStats=indexStats;
        this.baseTableStats=baseTableStats;
        this.indexSizeFactor = columnFraction;
        this.baseOverhead = baseOverhead;
        this.indexOverhead = indexOverhead;
    }

    @Override public long rowCount(){ return indexStats.rowCount(); }
    @Override public long totalSize(){ return indexStats.totalSize(); }
    @Override public long avgSize(){ return indexStats.avgSize(); }
    @Override public double localReadLatency(){ return indexStats.localReadLatency(); }
    @Override public int avgRowWidth(){
        double baseColsWidth = baseTableStats.avgRowWidth() - baseOverhead;
        double indexColsWidth = baseColsWidth * indexSizeFactor;
        int indexRowWidth = (int) (indexColsWidth + indexOverhead);
        return indexRowWidth < 1 ? 1 : indexRowWidth; // make sure we don't end up with a bogus width
    }
    @Override public long localReadTime(){ return indexStats.localReadTime(); }
    @Override public long numOpenEvents(){ return baseTableStats.numOpenEvents(); }
    @Override public long numCloseEvents(){ return baseTableStats.numCloseEvents(); }

    @Override public double openScannerLatency(){ return baseTableStats.openScannerLatency(); }
    @Override public double closeScannerLatency(){ return baseTableStats.closeScannerLatency(); }

    public double getBaseTableAvgRowWidth() {
        return baseTableStats.avgRowWidth();
    }

    @Override
    public double remoteReadLatency(){
        return baseTableStats.remoteReadLatency();
    }
    @Override
    public long remoteReadTime(){
        return (long)(indexSizeFactor*baseTableStats.remoteReadTime());
    }

    @Override public long queryCount(){ return indexStats.queryCount(); }
    @Override public String partitionId(){ return indexStats.partitionId(); }
    @Override public String tableId(){ return indexStats.tableId(); }
    @Override public long collectionTime(){ return indexStats.collectionTime(); }
    @Override public List<ColumnStatistics> columnStatistics(){ return indexStats.columnStatistics(); }
    @Override public <T> ColumnStatistics<T> columnStatistics(int columnId){ return indexStats.columnStatistics(columnId); }
    @Override public <T extends Comparable<T>> Distribution<T> columnDistribution(int columnId){ return indexStats.columnDistribution(columnId); }

    @Override
    public List<? extends PartitionStatistics> partitionStatistics(){
        if(indexPartStats==null){
            List<? extends PartitionStatistics> partitionStatisticses=indexStats.partitionStatistics();
            indexPartStats = new ArrayList<>(partitionStatisticses.size());
            for(PartitionStatistics stats:partitionStatisticses){
                indexPartStats.add(new IndexPartitionStatistics((OverheadManagedPartitionStatistics)stats,baseTableStats));
            }
        }
        return indexPartStats;
    }

    @Override
    public String toString() {
        return String.format("IndexTableStatistics(%s) {openScannerLatency=%f, closeScannerLatency=%f, numOpenEvents=%d, numCloseEvents=%d," +
                        "rowCounts=%d, totalSize=%d, avgSize=%d, localReadLatency=%f, remoteReadLatency=%f, localReadTime=%d, " +
                        "remoteReadTime=%d, avgRowWidth=%d, queryCount=%d}",
                tableId(),
                openScannerLatency(),
                closeScannerLatency(),
                numOpenEvents(),
                numCloseEvents(),
                rowCount(),
                totalSize(),
                avgSize(),
                localReadLatency(),
                remoteReadLatency(),
                localReadTime(),
                remoteReadTime(),
                avgRowWidth(),
                queryCount());
    }

    @Override
    public PartitionStatistics merge(PartitionStatistics other){
        indexStats.merge(other);
        return this;
    }

    public double multiGetLatency(){
        double avg = 0d;
        List<? extends PartitionStatistics> indexStats = partitionStatistics();
        if(indexStats.size()<=0) return 0d;
        for(PartitionStatistics pStats:indexStats){
            avg+=((IndexPartitionStatistics)pStats).multiGetLatency();
        }
        return avg/indexStats.size();
    }
}
