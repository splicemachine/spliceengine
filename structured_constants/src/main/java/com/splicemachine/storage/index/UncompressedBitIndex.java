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
    private final BitSet scalarFields;
    private final BitSet floatFields;
    private final BitSet doubleFields;

    private UncompressedBitIndex(BitSet bitSet, BitSet scalarFields,BitSet floatFields,BitSet doubleFields) {
        this.bitSet = bitSet;
        this.scalarFields = (BitSet)scalarFields.clone();
        this.scalarFields.and(bitSet);
        this.floatFields = (BitSet)floatFields.clone();
        this.floatFields.and(bitSet);
        this.doubleFields = (BitSet)doubleFields.clone();
        this.doubleFields.and(bitSet);
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
             * Because this field is present, we need to use 2 bits to indicate the
             * type information necessary to parse. The format for the type bit is
             *
             * Untyped: 00
             * Double: 01
             * Float: 10
             * Scalar: 11
             */
            if(scalarFields.get(setPos)){
                bitWriter.set(3);
            }else if(floatFields.get(setPos)){
                bitWriter.set(2);
                bitWriter.skipNext();
            }else if(doubleFields.get(setPos)){
                bitWriter.setNext();
                bitWriter.skipNext();
                bitWriter.setNext();
            }else{
                bitWriter.setNext();
                bitWriter.skip(2);
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
         *  to bitSet.length()). Because each set bit will have an additional 2-bit "type delimiter"
         *  set afterwords, we need to have 3 bits for every set bit, but 1 for every non-set bit
         *
         * This is equivalent to length()+2*numSetBits().
         *
         * we have 4 available bits in the header, and 7 bits in each subsequent byte (we use a continuation
         * bit).
         */
        int numBits = bitSet.length()+2*bitSet.cardinality();
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
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UncompressedBitIndex)) return false;

        UncompressedBitIndex that = (UncompressedBitIndex) o;

        return bitSet.equals(that.bitSet)
                && doubleFields.equals(that.doubleFields)
                && floatFields.equals(that.floatFields)
                && scalarFields.equals(that.scalarFields);

    }

    @Override
    public int hashCode() {
        int result = bitSet.hashCode();
        result = 31 * result + scalarFields.hashCode();
        result = 31 * result + floatFields.hashCode();
        result = 31 * result + doubleFields.hashCode();
        return result;
    }

    @Override
    public String toString() {
        return "{" +
                bitSet +
                "," + scalarFields +
                "," + floatFields +
                "," + doubleFields +
                '}';
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
    public boolean isScalarType(int position) {
        return scalarFields.get(position);
    }

    @Override
    public boolean isDoubleType(int position) {
        return doubleFields.get(position);
    }

    @Override
    public boolean isFloatType(int position) {
        return floatFields.get(position);
    }

    @Override
    public BitSet getScalarFields() {
        return scalarFields;
    }

    @Override
    public BitSet getDoubleFields() {
        return doubleFields;
    }

    @Override
    public BitSet getFloatFlields() {
        return floatFields;
    }

    @Override
    public BitSet and(BitSet bitSet) {
        final BitSet result = (BitSet) this.bitSet.clone();
        result.and(bitSet);
        return result;
    }

    public static BitIndex create(BitSet setCols,BitSet lengthDelimitedFields,BitSet floatFields,BitSet doubleFields) {
        return new UncompressedBitIndex(setCols,lengthDelimitedFields,floatFields,doubleFields);
    }

    public static BitIndex wrap(byte[] data, int position, int limit) {
        //create a BitSet underneath
        BitSet bitSet = new BitSet();
        BitSet scalarFields = new BitSet();
        BitSet floatFields = new BitSet();
        BitSet doubleFields = new BitSet();
        BitReader bitReader = new BitReader(data,position,limit,5,true);

        int bitPos = 0;
        while(bitReader.hasNext()){
            int zeros = bitReader.nextSetBit();
            if(zeros<0) break;
            bitPos+= zeros;
            bitSet.set(bitPos);
            if(bitReader.next()!=0){
                //either float or scalar
                if(bitReader.next()!=0){
                    scalarFields.set(bitPos);
                }else
                    floatFields.set(bitPos);
            }else{
                //either a double or untyped
                if(bitReader.next()!=0)
                    doubleFields.set(bitPos);
            }
            bitPos++;
        }
        return new UncompressedBitIndex(bitSet,scalarFields,floatFields,doubleFields);
    }

    public static void main(String... args) throws Exception{
        BitSet bitSet = new BitSet(11);
        bitSet.set(2);
        bitSet.set(1);
//        bitSet.set(3);
//        bitSet.set(4);

        BitSet scalarFields = new BitSet();
//        lengthDelimited.set(4);

        BitSet floatFields = new BitSet();

        BitSet doubleFields = new BitSet();

        BitIndex bits = UncompressedBitIndex.create(bitSet,scalarFields,floatFields,doubleFields);

        byte[] data =bits.encode();
        System.out.println(Arrays.toString(data));

        BitIndex decoded = UncompressedBitIndex.wrap(data,0,data.length);
        System.out.println(decoded);
    }
}
