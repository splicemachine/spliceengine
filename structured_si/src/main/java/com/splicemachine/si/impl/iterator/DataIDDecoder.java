package com.splicemachine.si.impl.iterator;

/**
 * Interface used to generically map between a Data object and its corresponding ID.
 */
public interface DataIDDecoder<ID, Data> {
    ID getID(Data data);
}
