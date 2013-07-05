package com.splicemachine.storage;

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
    public int cardinality() {
        return 0;
    }

}
