package com.splicemachine.si.api;

/**
 * Generate timestamps representing the time in milliseconds since the epoch.
 */
public interface Clock {
    long getTime();
}
