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
    public int encodedSize() {
        return 0;
    }

    @Override
    public int nextSetBit(int position) {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int cardinality() {
        return 0;
    }

    @Override
    public int cardinality(int position) {
        return 0;
    }

    public static SparseBitIndex create(BitSet setCols) {
        return new SparseBitIndex(setCols);
    }

    public static SparseBitIndex wrap(byte[] data,int position, int limit){
        return null;
    }
}
