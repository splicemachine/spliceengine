package com.splicemachine.storage.index;

import com.splicemachine.storage.BitReader;
import com.splicemachine.storage.BitWriter;

import java.util.Arrays;
import java.util.BitSet;

/**
 * Represents a Dense, Compressed BitSet.
 *
 * BitSets are compressed during the Encoding process using Run-Length Encoding, which is a
 * mechanism where each entry is stored, followed by a count of how many times it's duplicated. For example,
 * the bit sequence 0 11 000 11111 0 1 0 11 can be represented as 01 12 03 15 01 11 01 12 (E.g. the value,
 * followed by a count of how many times that value is repeated).
 *
 * This is often very efficient when there are highly skewed distributions within the BitSet--say, all
 * 1s at the beginning, and all zeros at the end, or some similar structure.
 *
 * Because there is no clear demarcation between the end of a Dense, Compressed bit index and the
 * start of another bitstream within the same stream/buffer, this implementation places a 1 into the
 * Most significant bit (leftmost bit) for every byte after the header.  This way, every byte is guaranteed
 * to be non-zero, at the expense of a slightly larger index.
 *
 * The counts are encoded using Elias Delta Encoding (which has nice size features, plus is re-used from
 * sparse implementations).
 *
 * @author Scott Fines
 * Created on: 7/5/13
 */
class DenseCompressedBitIndex implements BitIndex {
    private final BitSet bitSet;
    private final BitSet lengthDelimitedFields;

    DenseCompressedBitIndex(BitSet bitSet,BitSet lengthDelimitedFields){
        this.bitSet = bitSet;
        this.lengthDelimitedFields =lengthDelimitedFields;
    }

    public static BitIndex compress(BitSet bitSet,BitSet lengthDelimitedFields){
        return new DenseCompressedBitIndex(bitSet,lengthDelimitedFields);
    }

    public int length() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isSet(int pos) {
        return bitSet.get(pos);
    }

    @Override
    public byte[] encode() {
        byte[] bytes = new byte[encodedSize()];
        bytes[0] = (byte)0xC0;

        BitWriter bitWriter = new BitWriter(bytes,0,bytes.length,5,true);
        int onesCount=0;
        int lastSetBit = -1;

        int nextSetBit = bitSet.nextSetBit(0);
        boolean addZero=false;
        while(nextSetBit>=0){
            int numZeros = nextSetBit-lastSetBit-1;
            if(addZero){
                numZeros++;
                addZero=false;
            }
            if(numZeros<=0){
                onesCount++;
                if(lengthDelimitedFields.get(nextSetBit)){
                    onesCount++;
                }else{
                    addZero=true;
                }
                lastSetBit=nextSetBit;
                nextSetBit = bitSet.nextSetBit(nextSetBit+1);
                continue;
            }
            //our ones are terminated
            if(onesCount>0){
                bitWriter.setNext();
                DeltaCoding.encode(onesCount,bitWriter);
            }

            //write out the zeros count
            bitWriter.skipNext();
            DeltaCoding.encode(numZeros,bitWriter);

            lastSetBit = nextSetBit;
            nextSetBit = bitSet.nextSetBit(nextSetBit+1);
            onesCount=1;
            if(lengthDelimitedFields.get(lastSetBit))
                onesCount++;
            else{
                addZero=true;
            }
        }
        if(onesCount>0){
            bitWriter.setNext();
            DeltaCoding.encode(onesCount,bitWriter);
        }
        return bytes;
    }

    @Override
    public int encodedSize() {
        int onesCount=0;
        int lastSetBit = -1;

        int nextSetBit = bitSet.nextSetBit(0);
        int numBits=0;

        boolean addZero=false;
        while(nextSetBit>=0){
            int numZeros = nextSetBit-lastSetBit-1;
            if(addZero){
                numZeros++;
                addZero=false;
            }
            if(numZeros<=0){
                onesCount++;
                if(lengthDelimitedFields.get(nextSetBit)){
                    onesCount++;
                }else{
                    addZero=true;
                }
                lastSetBit=nextSetBit;
                nextSetBit = bitSet.nextSetBit(nextSetBit+1);
                continue;
            }
            //our ones are terminated
            if(onesCount>0){
                numBits+=DeltaCoding.getEncodedLength(onesCount)+1;
            }

            numBits+=DeltaCoding.getEncodedLength(numZeros)+1;

            lastSetBit = nextSetBit;
            nextSetBit = bitSet.nextSetBit(nextSetBit+1);
            onesCount=1;
            if(lengthDelimitedFields.get(lastSetBit))
                onesCount++;
            else{
                addZero=true;
            }
        }
        if(onesCount!=0){
            //last bit were 1s
            numBits+= DeltaCoding.getEncodedLength(onesCount)+1;
        }

        int length = numBits - 4; //four bits fit in the header
        int numBytes = length/7;
        if(length %7>0)
            numBytes++;

        numBytes++; //add the header byte
        return numBytes;
    }

    @Override
    public int nextSetBit(int position) {
        return bitSet.nextSetBit(position);
    }

    @Override
    public boolean intersects(BitSet bitSet) {
        return this.bitSet.intersects(bitSet);
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
    public boolean isLengthDelimited(int position) {
        throw new UnsupportedOperationException();
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
    public String toString() {
        return "{"+bitSet+","+lengthDelimitedFields+"}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DenseCompressedBitIndex)) return false;

        DenseCompressedBitIndex that = (DenseCompressedBitIndex) o;

        return bitSet.equals(that.bitSet) && lengthDelimitedFields.equals(that.lengthDelimitedFields);
    }

    @Override
    public int hashCode() {
        int result = bitSet.hashCode();
        result = 31 * result + lengthDelimitedFields.hashCode();
        return result;
    }

    public static BitIndex wrap(byte[] data, int offset, int length) {

        BitReader reader = new BitReader(data,offset,length,5,true);
        BitSet bitSet = new BitSet();
        BitSet lengthDelimitedFields = new BitSet();

        int setBitPos=0;
        boolean extraZero = false;
        while(reader.hasNext()){
            if(reader.next()!=0){
                //decode ones
                int numOnes = DeltaCoding.decode(reader);
                if(numOnes<0) break;
                int numBitsToSet;
                int numDelimitedFields;
                if(numOnes%2==0){
                    numBitsToSet = numOnes/2;
                    numDelimitedFields = numBitsToSet;
                }else{
                    numBitsToSet = (numOnes-1)/2+1;
                    numDelimitedFields = numOnes/2;
                    extraZero=true;
                }
                bitSet.set(setBitPos,setBitPos+numBitsToSet);
                lengthDelimitedFields.set(setBitPos,setBitPos+numDelimitedFields);
                setBitPos+=numBitsToSet;
            }else{
                //decode zeros
                int numZeros = DeltaCoding.decode(reader);
                if(numZeros<0)break;
                setBitPos+=numZeros;
                if(extraZero){
                    setBitPos--;
                    extraZero=false;
                }

            }
        }

        return new DenseCompressedBitIndex(bitSet,lengthDelimitedFields);
    }

    public static void main(String... args) throws Exception{
        BitSet set = new BitSet();
        set.set(0);
        set.set(1);
        set.set(2);
        set.set(3);
        set.set(4);
        set.set(8);

        BitSet lengthFields = new BitSet();
//        lengthFields.set(0);
        lengthFields.set(1);
//        lengthFields.set(2);
        lengthFields.set(3);
        lengthFields.set(4);
        lengthFields.set(8);

        BitIndex index = new DenseCompressedBitIndex(set,lengthFields);
        byte[] data  = index.encode();
        System.out.println(Arrays.toString(data));
        BitIndex decoded = DenseCompressedBitIndex.wrap(data,0,data.length);
        System.out.println(decoded);
    }
}
