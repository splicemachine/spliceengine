package com.splicemachine.si.data.light;

/**
 * Generate timestamps representing the time in milliseconds since the epoch.
 */
public interface Clock {
    long getTime();
}
