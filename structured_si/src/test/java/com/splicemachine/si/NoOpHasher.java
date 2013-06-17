package com.splicemachine.si;

import com.splicemachine.si.impl.Hasher;

public class NoOpHasher implements Hasher {

    @Override
    public Object toHashable(Object value) {
        return value;
    }

    @Override
    public Object fromHashable(Object value) {
        return value;
    }
}
