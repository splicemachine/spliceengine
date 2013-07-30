package com.splicemachine.si.impl;

/**
 * Allow an arbitrary object to be converted to/from an equivalent object that can be used in collections (e.g. it
 * implements hashCode and equals in a way that takes into accounts the value of the object rather than the identity).
 */
public interface Hasher<Data, Hashable extends Comparable> {
    Hashable toHashable(Data value);
    Data fromHashable(Hashable value);
}
