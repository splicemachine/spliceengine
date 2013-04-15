package com.splicemachine.si.impl;

import com.splicemachine.si.api.TimestampSource;

public class SimpleTimestampSource implements TimestampSource {
    private long id = 0;

    @Override
    public long nextTimestamp() {
        synchronized (this) {
            id = id + 1;
            return id;
        }
    }
}
