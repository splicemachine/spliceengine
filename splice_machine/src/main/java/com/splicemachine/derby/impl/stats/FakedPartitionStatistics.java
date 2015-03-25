package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.SimplePartitionStatistics;

import java.util.List;

/**
 * @author Scott Fines
 *         Date: 3/23/15
 */
public class FakedPartitionStatistics extends SimpleOverheadManagedPartitionStatistics{

    public FakedPartitionStatistics(String tableId,
                                    String partitionId,
                                    long rowCount,
                                    long totalBytes,
                                    long queryCount,
                                    long totalLocalReadTime,
                                    long totalRemoteReadLatency,
                                    long openScannerTimeMicros, long openScannerEvents,
                                    long closeScannerTimeMicros, long closeScannerEvents,
                                    List<ColumnStatistics> columnStatistics){
        super(tableId,
                partitionId,
                rowCount,
                totalBytes,
                queryCount,
                totalLocalReadTime,
                totalRemoteReadLatency,
                openScannerTimeMicros,
                openScannerEvents,
                closeScannerTimeMicros,
                closeScannerEvents,
                columnStatistics);
    }
}
