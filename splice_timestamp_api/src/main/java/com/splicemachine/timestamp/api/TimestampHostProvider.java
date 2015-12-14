package com.splicemachine.timestamp.api;

import com.splicemachine.timestamp.impl.TimestampIOException;

/**
 * Created by jleach on 12/9/15.
 */
public interface TimestampHostProvider {
    public String getHost() throws TimestampIOException;
    public int getPort();
}
