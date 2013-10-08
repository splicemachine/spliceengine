package com.splicemachine.si.impl.iterator;

/**
 * Interface used to generically map between a Data object and its corresponding ID.
 */
public interface DataIDDecoder<ID extends Comparable, Data> {
    ID getID(Data data);
}
