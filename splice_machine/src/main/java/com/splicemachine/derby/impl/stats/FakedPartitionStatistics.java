package com.splicemachine.derby.impl.stats;

import com.splicemachine.EngineDriver;
import com.splicemachine.stats.ColumnStatistics;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 3/23/15
 */
public class FakedPartitionStatistics extends SimpleOverheadManagedPartitionStatistics{

    public static FakedPartitionStatistics create(String tableId, String partitionId,
                                                                  long rowCount, long totalBytes,
                                                                  List<ColumnStatistics> columnStatistics){
        long fallbackLocalLatency =EngineDriver.driver().getConfiguration().getLong(StatsConfiguration.FALLBACK_LOCAL_LATENCY);
        long fallbackRemoteLatencyRatio =EngineDriver.driver().getConfiguration().getLong(StatsConfiguration.FALLBACK_REMOTE_LATENCY_RATIO);

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
