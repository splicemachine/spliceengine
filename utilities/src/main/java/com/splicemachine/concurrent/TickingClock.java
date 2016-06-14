package com.splicemachine.concurrent;

/**
 * @author Scott Fines
 *         Date: 8/14/15
 */
public interface TickingClock extends Clock{

    /**
     * Move forward the clock by {@code millis} milliseconds.
     *
     * @param millis the millisecond to move forward by
     * @return the value of the clock (in milliseconds) after ticking forward
     */
    long tickMillis(long millis);

    /**
     * Move forward the clock by {@code nanos} nanoseconds.
     *
     * @param nanos the nanos to move forward by
     * @return the value of the clock (in nanoseconds) after moving forward.
     */
    long tickNanos(long nanos);
}
