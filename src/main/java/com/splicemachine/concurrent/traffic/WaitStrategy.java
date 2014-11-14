package com.splicemachine.concurrent.traffic;

/**
 * Strategy to force a thread to wait for a fixed amount of time.
 *
 * @author Scott Fines
 *         Date: 11/13/14
 */
public interface WaitStrategy {

    void wait(int iterationCount,long nanosLeft,long minWaitNanos);
}
