package com.splicemachine.si;

import com.splicemachine.si.impl.Hasher;

public class NoOpHasher implements Hasher {

    @Override
    public Comparable toHashable(Object value) {
        return (Comparable) value;
    }

    @Override
    public Object fromHashable(Comparable value) {
        return value;
    }
}
