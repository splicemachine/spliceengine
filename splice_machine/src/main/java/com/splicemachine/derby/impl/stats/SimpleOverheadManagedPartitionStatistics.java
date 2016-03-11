package com.splicemachine.derby.impl.stats;

import java.util.List;

import com.splicemachine.EngineDriver;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.PartitionStatistics;
import com.splicemachine.stats.SimplePartitionStatistics;

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

    public static SimpleOverheadManagedPartitionStatistics create(String tableId, String partitionId,
                                                                  long rowCount, long totalBytes,
                                                                  List<ColumnStatistics> columnStatistics){
        long fallbackLocalLatency =EngineDriver.driver().getConfiguration().getFallbackLocalLatency();
        long fallbackRemoteLatencyRatio =EngineDriver.driver().getConfiguration().getFallbackRemoteLatencyRatio();

        return new SimpleOverheadManagedPartitionStatistics(tableId,partitionId,
                rowCount,totalBytes,
                fallbackLocalLatency,fallbackRemoteLatencyRatio,
                columnStatistics);
    }

    public SimpleOverheadManagedPartitionStatistics(String tableId,
                                                    String partitionId,
                                                    long rowCount,
                                                    long totalBytes,
                                                    long fallbackLocalLatency,
                                                    long fallbackRemoteLatencyRatio,
                                                    List<ColumnStatistics> columnStatistics){
        super(tableId,partitionId,rowCount,totalBytes,1,
                fallbackLocalLatency*rowCount,fallbackRemoteLatencyRatio*fallbackLocalLatency*rowCount,columnStatistics);
        this.totalOpenScannerTime = fallbackRemoteLatencyRatio*fallbackLocalLatency;
        this.totalCloseScannerTime = fallbackRemoteLatencyRatio*fallbackLocalLatency;
        this.numOpenEvents = 1L;
        this.numCloseEvents = 1L;
    }

    @Override
    public double getOpenScannerLatency(){

        if(numOpenEvents<=0) return 0d;
        return ((double)totalOpenScannerTime)/numOpenEvents;
    }

    @Override public long numOpenEvents(){ return numOpenEvents; }

    @Override public long numCloseEvents(){ return numCloseEvents; }

    @Override
    public double getCloseScannerLatency(){
        if(numCloseEvents<=0) return 0d;
        return ((double)totalCloseScannerTime)/numCloseEvents;
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
