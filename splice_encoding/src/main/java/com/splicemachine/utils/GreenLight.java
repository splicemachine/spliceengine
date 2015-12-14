package com.splicemachine.utils;

/**
 * Represents a Traffic Controller that always allows traffic through immediately.
 * @author Scott Fines
 *         Date: 12/9/14
 */
public class GreenLight implements TrafficControl{
    public static final GreenLight INSTANCE = new GreenLight();

    @Override public void release(int permits) {  }
    @Override public int tryAcquire(int minPermits, int maxPermits) { return maxPermits; }
    @Override public void acquire(int permits) throws InterruptedException { }
    @Override public int getAvailablePermits() { return Integer.MAX_VALUE; }
    @Override public int getMaxPermits() { return Integer.MAX_VALUE; }
    @Override public void setMaxPermits(int newMaxPermits) {  }
}
