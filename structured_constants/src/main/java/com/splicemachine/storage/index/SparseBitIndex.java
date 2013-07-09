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
        byte[] bytes = new byte[encodedSize()];

        /*
         * Zero is special, since it can't be encoded using Delta Encoding, we
         * need to use bit-5 in the header to determine if position zero is present
         * or not.
         */
        if(bitSet.get(0)){
            bytes[0] = 0x08;
        }

        int[] byteAndBitOffset = new int[]{0,6};
        for(int i=bitSet.nextSetBit(1);i>=0;i=bitSet.nextSetBit(i+1)){
            encode(bytes,i,byteAndBitOffset);
        }

        return bytes;
    }

    @Override
    public int encodedSize() {
        /*
         * Delta coding requires log2(x)+2*floor(log2(floor(log2(x))+1))+1 bits for each number, which helps
         * us to compute our size correctly
         */
        int numBits = 0;
        for(int i=bitSet.nextSetBit(1);i>=0;i=bitSet.nextSetBit(i+1)){
            //note that floor(log2(x)) = 31-numberOfLeadingZeros(i)
            int log2x = 31-Integer.numberOfLeadingZeros(i);
            numBits+=log2x;
            int log2x1 = 31-Integer.numberOfLeadingZeros(log2x+1);
            numBits+= 2*log2x1;
            numBits++;
        }

        int length = numBits-3; //3 bits are set in the header
        int numBytes = length/7;
        if(length%7!=0){
            numBytes++;
        }

        numBytes++; //add the header byte
        return numBytes;
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
        return bitSet.intersects(bitSet); //TODO -sf- do lazy decoding
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof SparseBitIndex)) return false;

        SparseBitIndex that = (SparseBitIndex) o;

        if (!bitSet.equals(that.bitSet)) return false;

        return true;
    }

    @Override
    public int hashCode() {
        return bitSet.hashCode();
    }

    @Override
    public String toString() {
        return bitSet.toString();
    }

    public static SparseBitIndex create(BitSet setCols) {
        return new SparseBitIndex(setCols);
    }

    public static SparseBitIndex wrap(byte[] data,int position, int limit){
        BitSet bitSet = new BitSet();

        //there are no entries
        if(data[position]==0x00)
            return new SparseBitIndex(bitSet);

        //check if the zero-bit is set
        if ((data[position] & 0x08) !=0){
            bitSet.set(0);
        }

        int[] byteAndBitOffset = new int[]{position,6};

        do{
            int val = decode(data, byteAndBitOffset);
            if(val>=0)
                bitSet.set(val);
            else
                break;
        }while(byteAndBitOffset[0]<position+limit);
        return new SparseBitIndex(bitSet);
    }

    private static int decode(byte[] data, int[] byteAndBitOffset) {
        int byteOffset = byteAndBitOffset[0];
        int bitPos = byteAndBitOffset[1];

        byte byt;
        if(bitPos==9){
            byteOffset++;
            if(byteOffset>=data.length)
                return -1;
            byt = data[byteOffset];
            bitPos=2;
        }else
            byt = data[byteOffset];
        int l = 0;
        while((byt & 1<<Byte.SIZE-bitPos)==0){
            l++;
            bitPos++;
            if(bitPos==9){
                byteOffset++;
                if(byteOffset>=data.length)
                    return -1; //we've exhausted the array, so there can't be any more data in the set
                byt = data[byteOffset];
                bitPos=2;
            }
        }
        bitPos++;
        //read the next l digits in
        int n = 1<<l;
        for(int i=1;i<=l;i++){
            if(bitPos==9){
                byteOffset++;
                byt = data[byteOffset];
                bitPos=2;
            }
            int val = byt & (1<< Byte.SIZE-bitPos);
            if(val!=0)
                n |= (1<<l-i);
            bitPos++;
        }
        n--;
        int retVal = 1<<n;
        //read remaining digits
        for(int i=1;i<=n;i++){
            if(bitPos==9){
                byteOffset++;
                byt = data[byteOffset];
                bitPos=2;
            }
            int val = byt & (1<<Byte.SIZE-bitPos);
            if(val!=0)
                retVal |= (1<<n-i);
            bitPos++;
        }
        byteAndBitOffset[0] = byteOffset;
        byteAndBitOffset[1] = bitPos;
        return retVal;
    }

    private static void encode(byte[] bytes,int val,int[] byteAndBitOffset) {
        int x = 32-Integer.numberOfLeadingZeros(val);
        int numZeros = 32-Integer.numberOfLeadingZeros(x)-1;

        int byteOffset = byteAndBitOffset[0];
        int bitPos = byteAndBitOffset[1];
        byte next = bytes[byteOffset];
        for(int i=0;i<numZeros;i++){
            if(bitPos==9){
                bytes[byteOffset]=next;
                byteOffset++;
                next = bytes[byteOffset];
                if(byteOffset>0)
                    next = (byte)0x80;
                bitPos=2;
            }
            bitPos++;
        }

        //append bits of x
        int numBitsToWrite = 32-Integer.numberOfLeadingZeros(x);
        for(int i=1;i<=numBitsToWrite;i++){
            if(bitPos==9){
                bytes[byteOffset]=next;
                byteOffset++;
                next = bytes[byteOffset];
                if(byteOffset>0)
                    next = (byte)0x80;
                bitPos=2;
            }
            int v = (x & (1<<numBitsToWrite-i));
            if(v!=0){
                next |= (1<<Byte.SIZE-bitPos);
            }
            bitPos++;
        }

        //append bits of y
        int pos = 1<<x-1;
        int y = val & ~pos;
        numBitsToWrite = 32-Integer.numberOfLeadingZeros(pos)-1;
        //y might be a bunch of zeros, so if y
        for(int i=1;i<=numBitsToWrite;i++){
            if(bitPos==9){
                bytes[byteOffset]=next;
                byteOffset++;
                next = bytes[byteOffset];
                if(byteOffset>0)
                    next = (byte)0x80;
                bitPos=2;
            }
            int v = (y & (1<<numBitsToWrite-i));
            if(v!=0){
                next |= (1<<Byte.SIZE-bitPos);
            }
            bitPos++;
        }
        if(bitPos!=2){
            bytes[byteOffset]=next;
        }
        byteAndBitOffset[0] = byteOffset;
        byteAndBitOffset[1] = bitPos;
    }

    public static void main(String... args) throws Exception{
        BitSet test = new BitSet();
        test.set(1);
        test.set(5);
        test.set(6);
        test.set(7);
        test.set(8);

        SparseBitIndex index = SparseBitIndex.create(test);
        byte[] encode = index.encode();
        SparseBitIndex index2 = SparseBitIndex.wrap(encode,0,encode.length);
        System.out.println(index2);
    }
}
