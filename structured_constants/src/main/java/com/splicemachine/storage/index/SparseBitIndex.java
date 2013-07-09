package com.splicemachine.storage.index;

import java.util.BitSet;

/**
 * A Sparse implementation of a BitIndex.
 *
 * The Encoding used here is the Elias Delta Coding, which has
 * near-optimal storage of non-negative integers. The approach is as follows
 *
 * <ol>
 *     <li>Split the number {@code N = 2^b+a}, with {@code a} and {@code b}
 *     nonnegative integers.
 *     </li>
 *     <li>Take {@code b+1}, and write it as {@code b+1 = 2^b' + a'}.</li>
 *     <li>write out {@code b'} zeros followed by a 1.</li>
 *     <li>write {@code a'}</li>
 *     <li>write {@code a}</li>
 * </ol>
 *
 * This is provably near-optimal,
 * in that it uses {@code log2(x) + 2*log2(log2(x)+1) + 1} bits to store a number,
 * and there are no wasted bits.
 *
 * @author Scott Fines
 * Created on: 7/5/13
 */
public class SparseBitIndex implements BitIndex {
    private final BitSet bitSet;

    public SparseBitIndex(BitSet setCols) {
        this.bitSet = setCols;
    }

    @Override
    public int length() {
        return bitSet.length();
    }

    @Override
    public boolean isSet(int pos) {
        return bitSet.get(pos);
    }

    @Override
    public byte[] encode() {
        for(int i=bitSet.nextSetBit(0);i>=0;i=bitSet.nextSetBit(i+1)){

        }
        throw new UnsupportedOperationException("Implement!");
    }

    @Override
    public int encodedSize() {
        return 0;
    }

    @Override
    public int nextSetBit(int position) {
        return bitSet.nextSetBit(position);
    }

    @Override
    public int cardinality() {
        return bitSet.cardinality();
    }

    @Override
    public int cardinality(int position) {
        int count=0;
        for(int i=bitSet.nextSetBit(0);i>=0 && i<position;i=bitSet.nextSetBit(i+1)){
            count++;
        }
        return count;
    }

    @Override
    public boolean intersects(BitSet bitSet) {
        return false;
    }

    @Override
    public BitSet and(BitSet bitSet) {
        throw new UnsupportedOperationException();
    }

    public static SparseBitIndex create(BitSet setCols) {
        return new SparseBitIndex(setCols);
    }

    public static SparseBitIndex wrap(byte[] data,int position, int limit){
        return null;
    }
}
