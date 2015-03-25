package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.PartitionStatistics;
import com.splicemachine.stats.SimplePartitionStatistics;

import java.util.List;

/**
 * @author Scott Fines
 *         Date: 3/25/15
 */
public class SimpleOverheadManagedPartitionStatistics extends SimplePartitionStatistics
                                                        implements OverheadManagedPartitionStatistics{
    private long totalOpenScannerTime;
    private long totalCloseScannerTime;
    private long numOpenEvents;
    private long numCloseEvents;

    public SimpleOverheadManagedPartitionStatistics(String tableId,
                                                    String partitionId,
                                                    long rowCount,
                                                    long totalBytes,
                                                    long queryCount,
                                                    long totalLocalReadTime,
                                                    long totalRemoteReadLatency,
                                                    long openScannerTimeMicros,
                                                    long openScannerEvents,
                                                    long closeScannerTimeMicros,
                                                    long closeScannerEvents,
                                                    List<ColumnStatistics> columnStatistics){
        super(tableId,partitionId,rowCount,totalBytes,queryCount,totalLocalReadTime,totalRemoteReadLatency,columnStatistics);
        this.totalOpenScannerTime = openScannerTimeMicros;
        this.totalCloseScannerTime = closeScannerTimeMicros;
        this.numOpenEvents = openScannerEvents;
        this.numCloseEvents = closeScannerEvents;
    }

    @Override
    public double getOpenScannerLatency(){
        if(numOpenEvents<=0) return 0d;
        return totalOpenScannerTime/numOpenEvents;
    }

    @Override public long numOpenEvents(){ return numOpenEvents; }

    @Override public long numCloseEvents(){ return numCloseEvents; }

    @Override
    public double getCloseScannerLatency(){
        if(numCloseEvents<=0) return 0d;
        return totalCloseScannerTime/numCloseEvents;
    }

    @Override
    public PartitionStatistics merge(PartitionStatistics other){
        //we know the SimplePartition
        PartitionStatistics merge=super.merge(other);
        assert merge==this: "Programmer error: SimplePartitionStatistics no longer returns the same instance.";

        if(other instanceof OverheadManagedPartitionStatistics){
            OverheadManagedPartitionStatistics omps = (OverheadManagedPartitionStatistics)other;
            //turn our latency measures into an average
            totalOpenScannerTime+=(long)(omps.getOpenScannerLatency()*omps.numOpenEvents());
            numOpenEvents+=omps.numOpenEvents();
            totalCloseScannerTime+=(long)(omps.getCloseScannerLatency()*omps.numCloseEvents());
            numCloseEvents+=omps.numCloseEvents();
        }
        return this;
    }
}
