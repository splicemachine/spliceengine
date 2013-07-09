package com.splicemachine.storage.index;

/**
 * Lazily decoding version of a Dense, Compressed Bit Index.
 *
 * @author Scott Fines
 * Created on: 7/8/13
 */
class LazyCompressedBitIndex extends LazyBitIndex{
    private int[] offsetAndBitPos;
    private int setBitPos=0;

    protected LazyCompressedBitIndex(byte[] encodedBitMap,
                                     int offset, int length) {
        super(encodedBitMap, offset, length);
        offsetAndBitPos = new int[]{offset,5};
    }

    @Override
    protected int decodeNext() {
        if(offsetAndBitPos[1]==9){
            offsetAndBitPos[0]++;
            if(offsetAndBitPos[0]>encodedBitMap.length||offsetAndBitPos[0]>=length)
                return -1;
            offsetAndBitPos[1] =2;
        }

        byte byt = encodedBitMap[offsetAndBitPos[0]];
        int val = (byt & (1<<Byte.SIZE-offsetAndBitPos[1]));
        offsetAndBitPos[1]++;
        if(offsetAndBitPos[1]==9){
            offsetAndBitPos[0]++;
            if(offsetAndBitPos[0]>=encodedBitMap.length||offsetAndBitPos[0]>=length)
               return -1;
            offsetAndBitPos[1] = 2;
        }
        if(val==0){
            int numZeros = DeltaCoding.decode(encodedBitMap,offsetAndBitPos);
            if(numZeros<0) return -1;
            setBitPos+=numZeros;
            return setBitPos;
        }else{
            int numOnes = DeltaCoding.decode(encodedBitMap,offsetAndBitPos);
            if(numOnes<0) return -1;
            decodedBits.set(setBitPos,setBitPos+numOnes);
            int bitPos = setBitPos;
            setBitPos+=numOnes;
            return bitPos;
        }
    }
}
