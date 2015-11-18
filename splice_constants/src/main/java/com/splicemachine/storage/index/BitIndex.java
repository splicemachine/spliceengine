package com.splicemachine.storage.index;

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.storage.Indexed;

/**
 *
 * Note: the first 4-bits should be ignored.
 *
 * @author Scott Fines
 * Created on: 7/5/13
 */
public interface BitIndex extends Indexed {

    /**
     * The "logical" length--the highest set bit + 1.
     *
     * @return the logical length of the index
     */
    int length();

    /**
     * @param pos the position to check (0-indexed).
     * @return true if the position is present in the index
     */
    boolean isSet(int pos);

    /**
     * Generate a byte representation of this Index for storage.
     *
     * @return a byte[] representation of this bit index.
     */
    byte[] encode();


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

    /**
     * @param bitSet the bitset to compare with
     * @return true if this index intersects with the specified BitSet. (e.g. If there are bits set in both
     * {@code bitSet} and this instance).
     */
    boolean intersects(BitSet bitSet);

    BitSet and(BitSet bitSet);

    boolean isEmpty();

    BitSet getScalarFields();

    BitSet getDoubleFields();

    BitSet getFloatFields();
}
