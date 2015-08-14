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

    private transient List<OverheadManagedPartitionStatistics> indexPartStats;

    public IndexTableStatistics(OverheadManagedTableStatistics indexStats,OverheadManagedTableStatistics baseTableStats){
        this.indexStats=indexStats;
        this.baseTableStats=baseTableStats;
        this.indexSizeFactor = ((double)indexStats.avgRowWidth())/baseTableStats.avgRowWidth();
    }

    @Override public long rowCount(){ return indexStats.rowCount(); }
    @Override public long totalSize(){ return indexStats.totalSize(); }
    @Override public long avgSize(){ return indexStats.avgSize(); }
    @Override public double localReadLatency(){ return indexStats.localReadLatency(); }
    @Override public int avgRowWidth(){ return indexStats.avgRowWidth(); }
    @Override public long localReadTime(){ return indexStats.localReadTime(); }
    @Override public long numOpenEvents(){ return baseTableStats.numOpenEvents(); }
    @Override public long numCloseEvents(){ return baseTableStats.numCloseEvents(); }

    @Override public double openScannerLatency(){ return baseTableStats.openScannerLatency(); }
    @Override public double closeScannerLatency(){ return baseTableStats.closeScannerLatency(); }

    @Override
    public double remoteReadLatency(){
        return indexSizeFactor*baseTableStats.remoteReadLatency();
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
                        "rowCounts=%d, totalSize=%d, avgSize=%f, localReadLatency=%f, remoteReadLatency=%f, localReadTime=%d, " +
                        "remoteReadTime=%d, avgRowWidth=%f, queryCount=%d}", tableId(), openScannerLatency(), closeScannerLatency(),
                numOpenEvents(), numCloseEvents(), rowCount(), totalSize(), avgSize(), localReadLatency(), remoteReadLatency(),
                localReadTime(), remoteReadTime(), avgRowWidth(), queryCount());
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
