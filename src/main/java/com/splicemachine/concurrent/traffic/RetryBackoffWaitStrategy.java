package com.splicemachine.concurrent.traffic;

import java.util.concurrent.locks.LockSupport;

/**
 * Uses exponential backoff to wait up to the specified timeout.
 * The strategy is as follows:
 *
 * @author Scott Fines
 *         Date: 11/13/14
 */
public class RetryBackoffWaitStrategy implements WaitStrategy {
    private final int windowSize;

    public RetryBackoffWaitStrategy(int windowSize) {
        int s = 1;
        while(s<windowSize){
            s<<=1;
        }
        this.windowSize = s;
    }

    @Override
    public void wait(int iterationCount, long nanosLeft, long minWaitNanos) {
        //take the highest power of two <= iterationCount, and that's the scale
        int scale = iterationCount/windowSize+1;

        long wait = Math.min(minWaitNanos*scale, nanosLeft);
        //now park for the minimum wait time
        LockSupport.parkNanos(wait);
    }
}
