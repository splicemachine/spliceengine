package com.splicemachine.storage.index;

import com.splicemachine.storage.BitReader;
import com.splicemachine.storage.BitWriter;

import com.carrotsearch.hppc.BitSet;


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
class SparseBitIndex implements BitIndex {
    private final BitSet bitSet;
    private final BitSet scalarFields;
    private final BitSet floatFields;
    private final BitSet doubleFields;

    private byte[] encodedVersion;

    private SparseBitIndex(BitSet setCols,BitSet scalarFields,BitSet floatFields,BitSet doubleFields ) {
        this.bitSet = setCols;
        this.scalarFields = scalarFields;
        this.floatFields = floatFields;
        this.doubleFields = doubleFields;
    }

    @Override
    public int length() {
        return (int) bitSet.length();
    }

    @Override
    public boolean isSet(int pos) {
        return bitSet.get(pos);
    }

    @Override
    public byte[] encode() {
        if(encodedVersion!=null) return encodedVersion;

        encodedVersion = new byte[encodedSize()];

        /*
         * Zero is special, since it can't be encoded using Delta Encoding, we
         * need to use bit-5 in the header to determine if position zero is present
         * or not.
         *
         * If it's present, we need to add the 2-bit type information:
         *
         * Untyped: 00
         * Double: 01
         * Float: 10
         * Scalar: 11
         */
        int initBitPos=6;
        if(bitSet.get(0)){
            initBitPos+=2;
            if(scalarFields!=null && scalarFields.get(0)){
                //set two bits after
                encodedVersion[0] =0x0E;
            }else if(floatFields!=null && floatFields.get(0)){
                encodedVersion[0] = 0x0C;
            }else if(doubleFields!=null && doubleFields.get(0)){
                encodedVersion[0] = 0x0A;
            }else
                encodedVersion[0] = 0x08;
        }

        BitWriter writer = new BitWriter(encodedVersion,0,encodedVersion.length,initBitPos,true);

        int lastSetPos=0;
        for(int i=bitSet.nextSetBit(1);i>=0;i=bitSet.nextSetBit(i+1)){
            int valueToEncode = i-lastSetPos;
            DeltaCoding.encode(valueToEncode,writer);
            if(scalarFields!=null && scalarFields.get(i))
                writer.set(2);
            else if(floatFields!=null && floatFields.get(i)){
                writer.setNext();
                writer.skipNext();
            }else if(doubleFields!=null && doubleFields.get(i)){
                writer.skipNext();
                writer.setNext();
            }else{
                writer.skip(2);
            }

            lastSetPos=i;
        }

        return encodedVersion;
    }

    @Override
    public int encodedSize() {
        /*
         * Delta coding requires log2(x)+2*floor(log2(floor(log2(x))+1))+1 bits for each number, which helps
         * us to compute our size correctly. For each set bit, we also need two bits to indicate the type
         * of the data:
         *
         * Untyped: 00
         * Double: 01
         * Float: 10
         * Scalar: 11
         */
        int numBits = 0;
        int lastSetPos=0;
        for(int i=bitSet.nextSetBit(1);i>=0;i=bitSet.nextSetBit(i+1)){
            int valToEncode = i-lastSetPos;
            int size = DeltaCoding.getEncodedLength(valToEncode);
            numBits+= size+2;
            lastSetPos=i;
        }

        int length = numBits-3;
        if(bitSet.get(0))
            length+=3; //reserve a bit for field information about position 0
        if(length<=0) return 1; //use only the header

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
        return (int) bitSet.cardinality();
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
    public BitSet and(BitSet bitSet) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isEmpty() {
        return bitSet.isEmpty();
    }

    @Override
    public boolean isScalarType(int position) {
        return scalarFields!=null && scalarFields.get(position);
    }

    @Override
    public boolean isDoubleType(int position) {
        return doubleFields!=null && doubleFields.get(position);
    }

    @Override
    public boolean isFloatType(int position) {
        return floatFields!=null && floatFields.get(position);
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

    public static SparseBitIndex create(BitSet setCols,BitSet scalarFields,BitSet floatFields,BitSet doubleFields) {
        BitSet cols = (BitSet)setCols.clone();
        BitSet sF = null;
        if(scalarFields!=null){
            sF = (BitSet)scalarFields.clone();
            sF.and(cols);
        }
        BitSet fF = null;
        if(floatFields!=null){
            fF = (BitSet)floatFields.clone();
            fF.and(cols);
        }
        BitSet dF = null;
        if(doubleFields!=null){
            dF = (BitSet)doubleFields.clone();
            dF.and(cols);
        }
        return new SparseBitIndex(cols,sF,fF,dF);
    }

    public static SparseBitIndex wrap(byte[] data,int position, int limit){

        BitSet bitSet = new BitSet();
        BitSet scalarFields = new BitSet();
        BitSet floatFields = new BitSet();
        BitSet doubleFields = new BitSet();

        //there are no entries
        if(data[position]==0x00)
            return new SparseBitIndex(bitSet,scalarFields,floatFields,doubleFields);

        //check if the zero-bit is set
        int startBitPos=6;
        if ((data[position] & 0x08) !=0){
            bitSet.set(0);
            startBitPos+=2;
            byte zeroByte = data[position];
            if((zeroByte & 0x04)!=0){
                //either scalar or float
                if((zeroByte & 0x02)!=0)
                    scalarFields.set(0);
                else
                    floatFields.set(0);
            }else{
                //either double or untyped
                if((zeroByte&0x02)!=0){
                    doubleFields.set(0);
                }
            }
        }

        int lastPosition=0;
        BitReader reader = new BitReader(data,position,limit,startBitPos,true);
        do{
            int val = DeltaCoding.decode(reader);
            if(val>=0){
                int pos = val+lastPosition;
                bitSet.set(pos);
                if(!reader.hasNext()){
                    //truncated type information--assume untyped
                    continue;
                }
                if(reader.next()!=0){
                    //either float or scalar
                    if(!reader.hasNext()){
                        //truncated type information--assume float
                        floatFields.set(pos);
                        continue;
                    }
                    if(reader.next()!=0)
                        scalarFields.set(pos);
                    else
                        floatFields.set(pos);
                }else{
                    //either double or untyped
                    if(!reader.hasNext()){
                        //truncated type information--assume untyped
                        continue;
                    }
                    if(reader.next()!=0)
                        doubleFields.set(pos);
                }
                lastPosition=pos;
            }
        }while(reader.hasNext());

        return new SparseBitIndex(bitSet,scalarFields,floatFields,doubleFields);
    }

    public static void main(String... args) throws Exception{
        BitSet test = new BitSet();
        test.set(0);
        test.set(1);
//        test.set(2);
//        test.set(3);
//        test.set(4);
//        test.set(5);
//        test.set(6);
//        test.set(7);
//        test.set(8);

        BitSet lengthFields = new BitSet();
        lengthFields.set(1);
//        lengthFields.set(3);
//        lengthFields.set(4);
//        lengthFields.set(5);
//        lengthFields.set(6);
//        lengthFields.set(7);

        BitSet floatFields = new BitSet();
        BitSet doubleFields = new BitSet();

        SparseBitIndex index = SparseBitIndex.create(test,lengthFields,floatFields,doubleFields);
        byte[] encode = index.encode();
        BitIndex index2 = BitIndexing.sparseBitMap(encode, 0, encode.length);
        for(int i=index2.nextSetBit(0);i>=0;i=index2.nextSetBit(i+1));
        System.out.println(index2);
    }
}
