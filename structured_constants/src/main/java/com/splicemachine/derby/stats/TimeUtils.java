package com.splicemachine.derby.stats;

/**
 * Utilities for dealing with Time stuff.
 *
 * @author Scott Fines
 * Created on: 2/27/13
 */
public class TimeUtils {

    private TimeUtils(){}

    private static final double NANOS_TO_MILLIS = 1000*1000d;
    private static final double NANOS_TO_SECONDS = 1000*NANOS_TO_MILLIS;

    /**
     * Converts nanoTime into a decimal seconds representation.
     *
     * @param nanoTime the time in nanoseconds
     * @return the time in seconds (with fractions of a second)
     */
    public static double toSeconds(long nanoTime){
        return nanoTime/NANOS_TO_SECONDS;
    }

    public static double toMillis(long nanoTime){
        return nanoTime/NANOS_TO_MILLIS;
    }

    public static double toMillis(double nanoTime){
        return nanoTime/NANOS_TO_MILLIS;
    }
}
