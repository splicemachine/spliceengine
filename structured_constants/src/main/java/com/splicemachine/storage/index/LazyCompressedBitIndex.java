package com.splicemachine.storage.index;

/**
 * Lazily decoding version of a Dense, Compressed Bit Index.
 *
 * @author Scott Fines
 * Created on: 7/8/13
 */
class LazyCompressedBitIndex extends LazyBitIndex{
    private int setBitPos=0;

    protected LazyCompressedBitIndex(byte[] encodedBitMap,
                                     int offset, int length) {
        super(encodedBitMap, offset, length,5);
    }

    @Override
    protected int decodeNext() {
        if(!bitReader.hasNext()) return -1;
        if(bitReader.next()==0){
            int numZeros = DeltaCoding.decode(bitReader);
            if(numZeros<0) return -1;
            setBitPos+=numZeros;
            return setBitPos;
        }else{
            int numOnes = DeltaCoding.decode(bitReader);
            if(numOnes<0) return -1;
            decodedBits.set(setBitPos,setBitPos+numOnes);
            int bitPos = setBitPos;
            setBitPos+=numOnes;
            return bitPos;
        }
    }

    @Override
    public boolean isLengthDelimited(int position) {
        throw new UnsupportedOperationException();
    }
}
