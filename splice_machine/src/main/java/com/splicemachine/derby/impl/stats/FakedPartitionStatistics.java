package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.SimplePartitionStatistics;

import java.util.List;

/**
 * @author Scott Fines
 *         Date: 3/23/15
 */
public class FakedPartitionStatistics extends SimplePartitionStatistics{
    public FakedPartitionStatistics(String tableId,
                                    String partitionId,
                                    long rowCount,
                                    long totalBytes,
                                    long queryCount,
                                    long totalLocalReadTime,
                                    long totalRemoteReadLatency,
                                    List<ColumnStatistics> columnStatistics){
        super(tableId,partitionId,rowCount,totalBytes,queryCount,totalLocalReadTime,totalRemoteReadLatency,columnStatistics);
    }
}
