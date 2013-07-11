package com.splicemachine.storage.index;

import java.util.Arrays;
import java.util.BitSet;

/**
 * Lazy implementation of an Uncompressed, Dense BitIndex.
 * @author Scott Fines
 * Created on: 7/8/13
 */
class UncompressedLazyBitIndex extends LazyBitIndex{

    int lastSetBit = -1;
    protected UncompressedLazyBitIndex(byte[] encodedBitMap,
                                       int offset, int length) {
        super(encodedBitMap, offset, length,5);
    }

    @Override
    protected int decodeNext() {
        if(!bitReader.hasNext()) return -1;
        int unsetCount = bitReader.nextSetBit();
        if(unsetCount<0) return -1;
        int pos = lastSetBit+unsetCount+1;
        if(!bitReader.hasNext())//assume that the next bit would be zero, which means unset
            return pos;

        if(bitReader.next()!=0){
            decodedLengthDelimitedFields.set(pos);
        }
        lastSetBit=pos;
        return pos;
    }


    public static void main(String... args) throws Exception{
        BitSet bitSet = new BitSet(11);
        bitSet.set(0);
        bitSet.set(1);
//        bitSet.set(3);
//        bitSet.set(4);

        BitSet lengthDelimited = new BitSet();
        lengthDelimited.set(0);

        BitIndex bits = UncompressedBitIndex.create(bitSet,lengthDelimited);

        byte[] data =bits.encode();
        System.out.println(Arrays.toString(data));

        BitIndex decoded = BitIndexing.uncompressedBitMap(data,0,data.length);
        for(int i=decoded.nextSetBit(0);i>=0;i=decoded.nextSetBit(i+1));

        System.out.println(decoded);
    }
}
