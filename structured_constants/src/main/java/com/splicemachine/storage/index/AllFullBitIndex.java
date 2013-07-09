package com.splicemachine.storage.index;

import java.util.BitSet;

/**
 * BitIndex representing an all-full bitmap.
 *
 * This allows for a compact representation of a bitmap which is
 * entirely composed of 1-entries. If there are any zeros, this won't
 * be correct.
 *
 * @author Scott Fines
 * Created on: 7/5/13
 */
public class AllFullBitIndex implements BitIndex {
    private static final byte encodedByte = (byte)(0x80 | 0x10);
    public static final BitIndex INSTANCE = new AllFullBitIndex();

    @Override
    public int length() {
        return 0;
    }

    @Override
    public boolean isSet(int pos) {
        return true;
    }

    @Override
    public byte[] encode() {
        return new byte[]{encodedByte};
    }

    @Override
    public int nextSetBit(int position) {
        return position;
    }

    @Override
    public int encodedSize() {
        return 1;
    }

    @Override
    public int cardinality() {
        return length();
    }

    @Override
    public int cardinality(int position) {
        return position; //all bits are set, so all bits < position are also set
    }

    @Override
    public boolean intersects(BitSet bitSet) {
        return true;
    }
}
