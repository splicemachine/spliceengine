package com.splicemachine.timestamp.api;

import com.splicemachine.timestamp.impl.TimestampIOException;

/**
 * Created by jleach on 12/9/15.
 */
public interface TimestampDataSource {
    public void reserveNextBlock(long currentMaxReserved) throws TimestampIOException;
    public long initialize() throws TimestampIOException;
}
