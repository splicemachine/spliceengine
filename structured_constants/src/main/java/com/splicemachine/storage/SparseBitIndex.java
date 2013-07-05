package com.splicemachine.storage;

import java.util.BitSet;

/**
 * @author Scott Fines
 *         Created on: 7/5/13
 */
public class SparseBitIndex implements BitIndex {
    public SparseBitIndex(BitSet setCols) {
        //To change body of created methods use File | Settings | File Templates.
    }

    @Override
    public int length() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isSet(int pos) {
        return false;
    }

    @Override
    public byte[] encode() {
        throw new UnsupportedOperationException("Implement!");
    }

    @Override
    public int cardinality() {
        return 0;
    }

    public static SparseBitIndex create(BitSet setCols) {
        return new SparseBitIndex(setCols);
    }

    public static SparseBitIndex wrap(byte[] data){
        return null;
    }
}
