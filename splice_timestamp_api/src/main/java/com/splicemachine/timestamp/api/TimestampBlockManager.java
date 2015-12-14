package com.splicemachine.timestamp.api;

/**
 * Manages how timestamp blocks are allocated to the TimestampOracle.
 *
 * Created by jleach on 12/9/15.
 */
public interface TimestampBlockManager{
    void reserveNextBlock(long currentMaxReserved) throws TimestampIOException;
    long initialize() throws TimestampIOException;
}
