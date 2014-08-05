package com.splicemachine.stats;

/**
 * @author Scott Fines
 *         Date: 7/21/14
 */
public interface LatencyTimer extends Timer {

    public DistributionTimeView getDistribution();
}
