package com.splicemachine.si.impl.iterator;

/**
 * Callbacks used by the ContiguousIterator.
 */
public interface ContiguousIteratorFunctions<ID, Data> {
    ID increment(ID ID);
    Data missing(ID ID);
}
