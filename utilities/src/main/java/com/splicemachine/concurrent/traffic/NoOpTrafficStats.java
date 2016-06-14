package com.splicemachine.concurrent.traffic;

/**
 * @author Scott Fines
 *         Date: 11/14/14
 */
public class NoOpTrafficStats implements TrafficStats{
    static final TrafficStats INSTANCE = new NoOpTrafficStats();

    @Override public long totalPermitsRequested() { return 0; }
    @Override public long totalPermitsGranted() { return 0; }
    @Override public double permitThroughput() { return 0; }
    @Override public double permitThroughput1M() { return 0; }
    @Override public double permitThroughput5M() { return 0; }
    @Override public double permitThroughput15M() { return 0; }
    @Override public long totalRequests() { return 0; }
    @Override public long avgRequestLatency() { return 0; }
}
