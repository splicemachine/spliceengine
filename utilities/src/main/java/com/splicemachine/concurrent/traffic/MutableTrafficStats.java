package com.splicemachine.concurrent.traffic;

import com.splicemachine.annotations.ThreadSafe;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A thread-safe, updateable version of TrafficStats. Useful
 * for keeping running statistics counts.
 *
 * @author Scott Fines
 *         Date: 11/14/14
 */
@ThreadSafe
public class MutableTrafficStats implements TrafficStats{
    AtomicLong totalRequests = new AtomicLong(0l);
    AtomicLong totalPermitsRequested = new AtomicLong(0l);
    AtomicLong totalPermitsGranted = new AtomicLong(0l);

    @Override public long totalPermitsRequested() { return totalPermitsRequested.get(); }
    @Override public long totalPermitsGranted() { return totalPermitsGranted.get(); }

    @Override
    public double permitThroughput() {
        return 0;
    }

    @Override
    public double permitThroughput1M() {
        return 0;
    }

    @Override
    public double permitThroughput5M() {
        return 0;
    }

    @Override
    public double permitThroughput15M() {
        return 0;
    }

    @Override
    public long totalRequests() {
        return 0;
    }

    @Override
    public long avgRequestLatency() {
        return 0;
    }
}
