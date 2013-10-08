package com.splicemachine.si.impl.iterator;

import java.io.IOException;

/**
 * Callbacks used by the ContiguousIterator.
 */
public interface ContiguousIteratorFunctions<ID, Data> {
    ID increment(ID ID);
    Data missing(ID ID) throws IOException;
}
