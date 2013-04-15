package com.splicemachine.si2.api;

/**
 * Generator of transaction timestamps.
 */
public interface TimestampSource {
    public long nextTimestamp();
}
