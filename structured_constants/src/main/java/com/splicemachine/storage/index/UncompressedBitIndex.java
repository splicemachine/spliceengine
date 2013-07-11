package com.splicemachine.storage.index;

import com.splicemachine.storage.BitReader;
import com.splicemachine.storage.BitWriter;

import java.util.Arrays;
import java.util.BitSet;

/**
 * Uncompressed, variable-length BitIndex.
 *
 * This index is represented as follows:
 *
 * The first four bits are header bits
 *
 * @author Scott Fines
 * Created on: 7/5/13
 */
class UncompressedBitIndex implements BitIndex {
    private final BitSet bitSet;
    private final BitSet lengthDelimitedBits;

    private UncompressedBitIndex(BitSet bitSet, BitSet lengthDelimitedBits) {
        this.bitSet = bitSet;
        this.lengthDelimitedBits = lengthDelimitedBits;
    }

    @Override
    public int length() {
        return bitSet.length();
    }

    @Override
    public int cardinality() {
        return bitSet.cardinality();
    }

    @Override
    public int cardinality(int position) {
        int count=0;
        for(int i=bitSet.nextSetBit(0);i>=0&&i<position;i=bitSet.nextSetBit(i+1)){
            count++;
        }
        return count;
    }

    @Override
    public boolean isSet(int pos) {
        return bitSet.get(pos);
    }

    @Override
    public byte[] encode() {
        byte[] bytes = new byte[encodedSize()];
        bytes[0] = (byte)0x80;
        BitWriter bitWriter = new BitWriter(bytes,0,bytes.length,5,true);
        int lastSetBit =-1;
        for(int setPos=bitSet.nextSetBit(0);setPos>=0;setPos=bitSet.nextSetBit(setPos+1)){
            //skip the distance between setPos and lastSetBit
            bitWriter.skip(setPos-lastSetBit-1);
            /*
             * Because this field is present, we need to use a bit to indicate whether or not
             * the field is length-delimited.
             */
            if(lengthDelimitedBits.get(setPos)){
                bitWriter.set(2);
            }else{
                bitWriter.setNext();
                bitWriter.skipNext();
            }
            lastSetBit=setPos;
        }
        return bytes;
    }

    @Override
    public int nextSetBit(int position) {
        return bitSet.nextSetBit(position);
    }

    @Override
    public int encodedSize() {
        /*
         * The number of bytes goes as follows:
         *
         * you need at least as many bits as the highest 1-bit in the bitSet(equivalent
         *  to bitSet.length()). Because each set bit will have an additional "length delimiter"
         *  bit set afterwords, we need to have 2 bits for every set bit, but 1 for every non-set bit
         *
         * This is equivalent to length()+numSetBits().
         *
         * we have 4 available bits in the header, and 7 bits in each subsequent byte (we use a continuation
         * bit).
         */
        int numBits = bitSet.length()+bitSet.cardinality();
        int numBytes = 1;
        numBits-=4;
        if(numBits>0){
           numBytes+=numBits/7;
            if(numBits%7!=0)
                numBytes++;
        }

        return numBytes;
    }

    @Override
    public String toString() {
        return "{"+bitSet.toString()+","+lengthDelimitedBits.toString()+"}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UncompressedBitIndex)) return false;

        UncompressedBitIndex that = (UncompressedBitIndex) o;

        return bitSet.equals(that.bitSet) && lengthDelimitedBits.equals(that.lengthDelimitedBits);

    }

    @Override
    public int hashCode() {
        int result = bitSet.hashCode();
        result = 31 * result + lengthDelimitedBits.hashCode();
        return result;
    }

    @Override
    public boolean intersects(BitSet bitSet) {
        return this.bitSet.intersects(bitSet);
    }

    @Override
    public boolean isEmpty() {
        return bitSet.isEmpty();
    }

    @Override
    public boolean isLengthDelimited(int position) {
        return lengthDelimitedBits.get(position);
    }

    @Override
    public BitSet and(BitSet bitSet) {
        final BitSet result = (BitSet) this.bitSet.clone();
        result.and(bitSet);
        return result;
    }

    public static BitIndex create(BitSet setCols,BitSet lengthDelimitedFields) {
        return new UncompressedBitIndex(setCols,lengthDelimitedFields);
    }

    public static BitIndex wrap(byte[] data, int position, int limit) {
        //create a BitSet underneath
        BitSet bitSet = new BitSet();
        BitSet lengthDelimitedFields = new BitSet();
        BitReader bitReader = new BitReader(data,position,limit,5,true);

        int bitPos = 0;
        while(bitReader.hasNext()){
            int zeros = bitReader.nextSetBit();
            if(zeros<0) break;
            bitPos+= zeros;
            bitSet.set(bitPos);
            if(bitReader.next()!=0){
                lengthDelimitedFields.set(bitPos);
            }
            bitPos++;
        }
        return new UncompressedBitIndex(bitSet,lengthDelimitedFields);
    }

    public static void main(String... args) throws Exception{
        BitSet bitSet = new BitSet(11);
        bitSet.set(2);
        bitSet.set(1);
//        bitSet.set(3);
//        bitSet.set(4);

        BitSet lengthDelimited = new BitSet();
//        lengthDelimited.set(4);

        BitIndex bits = UncompressedBitIndex.create(bitSet,lengthDelimited);

        byte[] data =bits.encode();
        System.out.println(Arrays.toString(data));

        BitIndex decoded = UncompressedBitIndex.wrap(data,0,data.length);
        System.out.println(decoded);
    }
}
