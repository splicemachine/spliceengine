package com.splicemachine.si.impl;

public interface Hasher<Data, Hashable> {
    Hashable toHashable(Data value);
    Data fromHashable(Hashable value);
}
