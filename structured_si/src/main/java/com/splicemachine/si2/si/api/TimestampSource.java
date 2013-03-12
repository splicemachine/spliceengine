package com.splicemachine.si2.si.api;

/**
 * Generator of transaction timestamps.
 */
public interface TimestampSource {
    public long nextTimestamp();
}
