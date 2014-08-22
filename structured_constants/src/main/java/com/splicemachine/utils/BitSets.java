package com.splicemachine.utils;

import com.carrotsearch.hppc.BitSet;

/**
 * Convenience methods for working with com.carrotsearch.hppc.BitSet.
 */
public class BitSets {

    private BitSets() {
    }

    /**
     * Create with bits from var-arg parameter set.
     */
    public static BitSet newBitSet(int... bits) {
        BitSet bitSet = new BitSet();
        for (int bit : bits) {
            bitSet.set(bit);
        }
        return bitSet;
    }
}
