package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.TableStatistics;

/**
 * @author Scott Fines
 *         Date: 3/25/15
 */
public interface OverheadManagedTableStatistics extends TableStatistics{

    /**
     * @return the latency to open a scanner against this table (in microseconds), averaged
     * over all partitions
     */
    double openScannerLatency();

    /**
     * @return the latency to close a scanner against this table (in microseconds), averaged
     * over all partitions
     */
    double closeScannerLatency();
}
