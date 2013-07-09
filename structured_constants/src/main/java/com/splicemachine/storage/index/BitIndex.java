package com.splicemachine.storage.index;

import java.util.BitSet;

/**
 *
 * Note: the first 4-bits should be ignored.
 *
 * @author Scott Fines
 * Created on: 7/5/13
 */
public interface BitIndex {

    /**
     * The "logical" length--the highest set bit + 1.
     *
     * @return the logical length of the index
     */
    int length();

    boolean isSet(int pos);

    byte[] encode();

    /**
     * Returns the next set bit at or higher than {@code position}.
     *
     * @param position the position to start from
     * @return the index of the next set bit that has index equal to or higher than {@code position}
     */
    int nextSetBit(int position);

    /**
     * Determines the encoded size of the index.
     *
     * @return the encoded size of the index
     */
    int encodedSize();

    /**
     * @return the number of set values in the index. Equivalent to cardinality(length()), but may
     * be more efficient
     */
    int cardinality();

    /**
     * @param position the position to check
     * @return the number of set values less than {@code position}
     */
    int cardinality(int position);

    boolean intersects(BitSet bitSet);

    BitSet and(BitSet bitSet);

}
