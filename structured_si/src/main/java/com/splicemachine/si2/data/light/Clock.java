package com.splicemachine.si2.data.light;

/**
 * Generate timestamps representing the time in milliseconds since the epoch.
 */
public interface Clock {
    long getTime();
}
