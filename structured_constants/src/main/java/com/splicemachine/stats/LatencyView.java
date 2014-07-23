package com.splicemachine.stats;

/**
 * @author Scott Fines
 *         Date: 7/21/14
 */
public interface LatencyView {

    public double getOverallLatency();
    public long getP25Latency();
    public long getP50Latency();
    public long getP75Latency();
    public long getP90Latency();
    public long getP95Latency();
    public long getP99Latency();

    public long getMinLatency();
    public long getMaxLatency();
}
