package com.splicemachine.si.api;

/**
 * Generator of transaction timestamps. Expected to produce unique, monotonically increasing values.
 */
public interface TimestampSource {
    long nextTimestamp();
    void rememberTimestamp(long timestamp);
    long retrieveTimestamp();
}
