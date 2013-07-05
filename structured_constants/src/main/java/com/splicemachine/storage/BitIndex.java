package com.splicemachine.storage;

/**
 *
 * Note: the first 4-bits should be ignored.
 *
 * @author Scott Fines
 * Created on: 7/5/13
 */
interface BitIndex {

    int length();

    boolean isSet(int pos);

    byte[] encode();

    /**
     * @return the number of set values in the index
     */
    int cardinality();
}
