package com.splicemachine.metrics;

/**
 * @author Scott Fines
 *         Date: 7/21/14
 */
public interface DistributionTimeView extends TimeView{

    public LatencyView wallLatency();
    public LatencyView cpuLatency();
    public LatencyView userLatency();
}
