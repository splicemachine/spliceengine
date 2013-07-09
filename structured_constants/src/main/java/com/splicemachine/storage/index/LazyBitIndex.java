package com.splicemachine.storage.index;

import java.util.BitSet;

/**
 * BitIndex which lazily decodes entries as needed, and which does not re-encode entries.
 *
 * @author Scott Fines
 * Created on: 7/8/13
 */
abstract class LazyBitIndex implements BitIndex{
    protected BitSet decodedBits;

    protected byte[] encodedBitMap;
    protected int offset;
    protected int length;

    protected LazyBitIndex(byte[] encodedBitMap,int offset,int length){
        this.encodedBitMap = encodedBitMap;
        this.offset = offset;
        this.length = length;
        this.decodedBits = new BitSet();
    }

    @Override
    public int length() {
        decodeAll();
        return decodedBits.length();
    }

    private void decodeAll() {
        int next;
        while((next = decodeNext()) >=0){
            decodedBits.set(next);
        }
    }

    protected abstract int decodeNext();

    @Override
    public boolean isSet(int pos) {
        decodeUntil(pos);
        return decodedBits.get(pos);
    }

    private void decodeUntil(int pos) {
        while(decodedBits.length()<pos){
            int next = decodeNext();
            if(next <0) return; //out of data to decode

            decodedBits.set(next);
        }
    }

    @Override
    public int nextSetBit(int position) {
        decodeUntil(position);
        int count=0;
        for(int i=decodedBits.nextSetBit(0);i>=0;i=decodedBits.nextSetBit(i+1)){
            count++;
        }
        return count;
    }

    @Override
    public int cardinality() {
        decodeAll();
        return decodedBits.cardinality();
    }

    @Override
    public int cardinality(int position) {
        decodeUntil(position);
        return decodedBits.cardinality();
    }

    @Override
    public boolean intersects(BitSet bitSet) {
        decodeUntil(bitSet.length());
        return decodedBits.intersects(bitSet);
    }

    @Override
    public BitSet and(BitSet bitSet) {
        decodeUntil(bitSet.length());
        final BitSet bits = (BitSet) decodedBits.clone();
        bits.and(bitSet);
        return bits;
    }

    @Override
    public byte[] encode() {
        byte[] copy = new byte[length];
        System.arraycopy(encodedBitMap,offset,copy,0,copy.length);
        return copy;
    }

    @Override
    public int encodedSize() {
        return length;
    }
}

