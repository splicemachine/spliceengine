package com.splicemachine.si.impl.store;

import com.splicemachine.timestamp.api.TimestampSource;

/**
 * @author Scott Fines
 *         Date: 12/17/15
 */
public class TestingTimestampSource implements TimestampSource{
    private long id = 0;
    private long memory = 0;

    @Override
    public long nextTimestamp() {
        synchronized (this) {
            id = id + 1;
            return id;
        }
    }

    @Override
    public void rememberTimestamp(long timestamp) {
        memory = timestamp;
    }

    @Override
    public long retrieveTimestamp() {
        return memory;
    }

    @Override
    public void shutdown() {

    }
}
