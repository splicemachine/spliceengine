package com.splicemachine.si;

import com.splicemachine.si.api.TimestampSource;

public class SimpleTimestampSource implements TimestampSource {
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
}
