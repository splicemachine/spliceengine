package com.splicemachine.derby.hbase;

/**
 * @author Scott Fines
 *         Date: 11/25/14
 */
public interface TrafficControl  {

    public void release(int permits);

    public int tryAcquire(int minPermits, int maxPermits);

    int getAvailablePermits();

    int getMaxPermits();

    void setMaxPermits(int newMaxPermits);
}
