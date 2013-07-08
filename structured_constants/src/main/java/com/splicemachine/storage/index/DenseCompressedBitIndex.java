package com.splicemachine.storage.index;

import java.util.BitSet;

/**
 * Represents a Compressed BitSet
 *
 * @author Scott Fines
 * Created on: 7/5/13
 */
public class DenseCompressedBitIndex implements BitIndex {

    private DenseCompressedBitIndex(BitSet bitSet){

    }

    public static BitIndex compress(BitSet bitSet){
        return new DenseCompressedBitIndex(bitSet);
    }

    public int length() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isSet(int pos) {
        return false;
    }

    @Override
    public byte[] encode() {
        throw new UnsupportedOperationException();
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

    public static BitIndex wrap(byte[] data, int offset, int length) {
        return null;  //To change body of created methods use File | Settings | File Templates.
    }
}
