package com.splicemachine.si2.si.impl;

import com.splicemachine.si2.si.api.IdSource;

public class SimpleIdSource implements IdSource {
    private long id = 0;

    @Override
    public long nextId() {
        synchronized (this) {
            id = id + 1;
            return id;
        }
    }
}
