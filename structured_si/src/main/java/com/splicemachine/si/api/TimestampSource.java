package com.splicemachine.si.api;

/**
 * Generator of transaction timestamps.
 */
public interface TimestampSource {
    public long nextTimestamp();
}
