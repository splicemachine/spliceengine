package com.splicemachine.si.api;

/**
 * Generator of transaction timestamps. Expected to produce unique, monotonically increasing values.
 */
public interface TimestampSource {
    public long nextTimestamp();
}
