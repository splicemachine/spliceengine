package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.PartitionStatistics;

/**
 * @author Scott Fines
 * Date: 3/25/15
 */
public interface OverheadManagedPartitionStatistics extends PartitionStatistics{

    /**
     * @return the latency (in microseconds) to open a new scanner
     */
    double getOpenScannerLatency();

    /**
     * @return the latency (in microseconds) to close a scanner
     */
    double getCloseScannerLatency();

    /**
     * @return the number of Scanner opens that were recorded
     */
    long numOpenEvents();

    /**
     * @return the number of Scanner closes that were recorded
     */
    long numCloseEvents();

}
