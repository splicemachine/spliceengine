package com.splicemachine.storage.index;

/**
 * Lazily decoding version of a Dense, Compressed Bit Index.
 *
 * @author Scott Fines
 * Created on: 7/8/13
 */
class LazyCompressedBitIndex extends LazyBitIndex{
    private int setBitPos=0;

    private boolean extraZero=false;
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
            if(extraZero){
                setBitPos--;
                extraZero=false;
            }
            //next block is a block of 1s, which has to be decoded to make sure
            //that we can set the length delimiter appropriately
            if(!bitReader.hasNext()){
                //assume the last position is zero, so we're good
                return setBitPos;
            }

            bitReader.next(); //skip the one separator
            int numOnes = DeltaCoding.decode(bitReader);
            if(numOnes<0) {
                //assume the last position is zero, so this isn't a length-delimited field
                return setBitPos;
            }
            return decodeAndSetOnes(numOnes);
        }else{
            int numOnes = DeltaCoding.decode(bitReader);
            if(numOnes<0) return -1;
            return decodeAndSetOnes(numOnes);
        }
    }

    @Override
    public boolean isLengthDelimited(int position) {
        throw new UnsupportedOperationException();
    }

    private int decodeAndSetOnes(int numOnes){
        int bitsToSet;
        int lengthFields;
        if(numOnes%2==0){
            bitsToSet = numOnes/2;
            lengthFields = bitsToSet;
        }else{
            bitsToSet = (numOnes-1)/2+1;
            lengthFields = numOnes/2;
            extraZero=true;
        }
        decodedBits.set(setBitPos,setBitPos+bitsToSet);
        decodedLengthDelimitedFields.set(setBitPos,setBitPos+lengthFields);
        int bitPos = setBitPos;
        setBitPos+=bitsToSet;

        return bitPos;
    }
}
