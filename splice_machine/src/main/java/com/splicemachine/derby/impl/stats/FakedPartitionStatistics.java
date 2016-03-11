package com.splicemachine.derby.impl.stats;

import java.util.List;

import com.splicemachine.EngineDriver;
import com.splicemachine.stats.ColumnStatistics;

/**
 * @author Scott Fines
 *         Date: 3/23/15
 */
public class FakedPartitionStatistics extends SimpleOverheadManagedPartitionStatistics{

    public static FakedPartitionStatistics create(String tableId, String partitionId,
                                                                  long rowCount, long totalBytes,
                                                                  List<ColumnStatistics> columnStatistics){
        long fallbackLocalLatency =EngineDriver.driver().getConfiguration().getFallbackLocalLatency();
        long fallbackRemoteLatencyRatio =EngineDriver.driver().getConfiguration().getFallbackRemoteLatencyRatio();

        return new FakedPartitionStatistics(tableId,partitionId,
                rowCount,totalBytes,
                fallbackLocalLatency,fallbackRemoteLatencyRatio,
                columnStatistics);
    }

    public FakedPartitionStatistics(String tableId,String partitionId,
                                    long rowCount,long totalBytes,
                                    long fallbackLocalLatency,long fallbackRemoteLatencyRatio,
                                    List<ColumnStatistics> columnStatistics){
        super(tableId,partitionId,rowCount,totalBytes,fallbackLocalLatency,fallbackRemoteLatencyRatio,columnStatistics);
    }
}
