package com.splicemachine.storage.index;

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

    DenseCompressedBitIndex(BitSet bitSet){
        this.bitSet = bitSet;
    }

    public static BitIndex compress(BitSet bitSet){
        return new DenseCompressedBitIndex(bitSet);
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

        int onesSequence = 0;
        bytes[0] = (byte)0xC0;
        byte byt = bytes[0];
        //fill the header
        int bitPos = 5;
        int[] offsetAndBitPos = new int[]{0,bitPos};
        int lastSetBit;
        if(bitSet.get(0)){
            lastSetBit=0;
        }else{
            lastSetBit=-1;
        }
        for(int nextSetBit = bitSet.nextSetBit(0);nextSetBit>=0;nextSetBit=bitSet.nextSetBit(nextSetBit+1)){
            int numZeros = nextSetBit-lastSetBit-1;
            if(numZeros<=0){
                onesSequence++;
            }else{
                //write out the ones that exist
                if(onesSequence>0){
                    if(bitPos==9){
                        bytes[offsetAndBitPos[0]] = byt;
                        offsetAndBitPos[0]++;
                        byt = bytes[offsetAndBitPos[0]] = (byte)0x80;
                        bitPos=2;
                    }
                    //set a 1-delimiter
                    byt |= (1<<Byte.SIZE-bitPos);
                    bitPos++;
                    offsetAndBitPos[1] = bitPos;
                    //write out the number of 1s in Delta code
                    bytes[offsetAndBitPos[0]] = byt;
                    DeltaCoding.encode(bytes,onesSequence,offsetAndBitPos);
                    bitPos = offsetAndBitPos[1];
                    byt = bytes[offsetAndBitPos[0]];
                }

                //write out the zeros
                if(bitPos==9){
                    bytes[offsetAndBitPos[0]] = byt;
                    offsetAndBitPos[0]++;
                    byt = bytes[offsetAndBitPos[0]] = (byte)0x80;
                    bitPos=2;
                }
                bitPos++;
                offsetAndBitPos[1] = bitPos;
                bytes[offsetAndBitPos[0]] = byt;
                DeltaCoding.encode(bytes,numZeros,offsetAndBitPos);
                bitPos = offsetAndBitPos[1];
                byt = bytes[offsetAndBitPos[0]];
                onesSequence=1;
            }
            lastSetBit=nextSetBit;
        }
        if(onesSequence!=0){
            if(bitPos==9){
                bytes[offsetAndBitPos[0]] = byt;
                offsetAndBitPos[0]++;
                byt = bytes[offsetAndBitPos[0]] = (byte)0x80;
                bitPos=2;
            }
            byt |= (1<<Byte.SIZE-bitPos);
            bitPos++;
            offsetAndBitPos[1] = bitPos;
            bytes[offsetAndBitPos[0]] = byt;
            DeltaCoding.encode(bytes,onesSequence,offsetAndBitPos);
        }
        return bytes;
    }

    @Override
    public int encodedSize() {
        int lastSetBit;
        int numBits=0;
        int onesSequence = 0;
        if(bitSet.get(0)){
            lastSetBit=0;
        }else{
            lastSetBit=-1;
        }

        for(int nextSetBit = bitSet.nextSetBit(0);nextSetBit>=0;nextSetBit=bitSet.nextSetBit(nextSetBit+1)){
            int numZeros = nextSetBit-lastSetBit-1;
            if(numZeros<=0){
                onesSequence++;
            }else{
                /*
                 *There were zeros in between the last ones. Thus, take the onesSequence and write out its
                 * length, then reset it to zero. Since we use Delta encoding to
                 * do the actual run length encoding, we add in those values as well
                 *
                 */
                if(onesSequence>0)
                    numBits+= DeltaCoding.getEncodedLength(onesSequence)+1;

                numBits+=DeltaCoding.getEncodedLength(numZeros)+1;
                onesSequence=1;
            }
            lastSetBit = nextSetBit;
        }
        if(onesSequence!=0){
            //last bit were 1s
            numBits+= DeltaCoding.getEncodedLength(onesSequence)+1;
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
        return bitSet.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof DenseCompressedBitIndex)) return false;

        DenseCompressedBitIndex that = (DenseCompressedBitIndex) o;

        return bitSet.equals(that.bitSet);
    }

    @Override
    public int hashCode() {
        return bitSet.hashCode();
    }

    public static BitIndex wrap(byte[] data, int offset, int length) {
        int[] offsetAndBitPos = new int[]{offset,5};

        BitSet bitSet = new BitSet();
        byte byt = data[offsetAndBitPos[0]];
        int setBitPos = 0;
        while(offsetAndBitPos[0]<length){
            if(offsetAndBitPos[1]==9){
                offsetAndBitPos[0]++;
                if(offsetAndBitPos[0]>=data.length||offsetAndBitPos[0]>=length)
                    break;
                byt = data[offsetAndBitPos[0]];
                offsetAndBitPos[1] =2;
            }
            //read the position to determine what type it is
            int val = (byt & (1<<Byte.SIZE-offsetAndBitPos[1]));
            offsetAndBitPos[1]++;
            if(offsetAndBitPos[1]==9){
                offsetAndBitPos[0]++;
                if(offsetAndBitPos[0]>=data.length||offsetAndBitPos[0]>=length)
                    break;
                offsetAndBitPos[1] = 2;
            }
            if(val==0){
                int numZeros = DeltaCoding.decode(data,offsetAndBitPos);
                if(numZeros<0) break; //we're out of data in the index
                setBitPos+=numZeros;
            }else{
                //read the range
                int numOnes = DeltaCoding.decode(data,offsetAndBitPos);
                if(numOnes<0) break; //we're out of data in the index
                bitSet.set(setBitPos,setBitPos+numOnes);
                setBitPos+=numOnes;
            }
            byt = data[offsetAndBitPos[0]];

        }

        return new DenseCompressedBitIndex(bitSet);
    }

    public static void main(String... args) throws Exception{
        BitSet set = new BitSet();
        set.set(0);
        set.set(3);

        BitIndex index = new DenseCompressedBitIndex(set);
        byte[] data  = index.encode();
        System.out.println(Arrays.toString(data));
        BitIndex decoded = DenseCompressedBitIndex.wrap(data,0,data.length);
        System.out.println(decoded);
    }
}
