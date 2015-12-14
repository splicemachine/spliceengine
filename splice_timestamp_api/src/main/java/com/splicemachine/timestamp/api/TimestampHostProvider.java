package com.splicemachine.timestamp.api;

/**
 * Created by jleach on 12/9/15.
 */
public interface TimestampHostProvider {
    String getHost() throws TimestampIOException;
    int getPort();
}
