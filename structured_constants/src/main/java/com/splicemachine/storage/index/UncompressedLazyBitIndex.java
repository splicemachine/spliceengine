package com.splicemachine.storage.index;

/**
 * Lazy implementation of an Uncompressed, Dense BitIndex.
 * @author Scott Fines
 * Created on: 7/8/13
 */
class UncompressedLazyBitIndex extends LazyBitIndex{

    private int bytePosition;
    private int bitPos = 5;

    private int highestFound= 0;
    protected UncompressedLazyBitIndex(byte[] encodedBitMap,
                                       int offset, int length) {
        super(encodedBitMap, offset, length);
        this.bytePosition = offset;
    }

    @Override
    protected int decodeNext() {
        byte byt = encodedBitMap[bytePosition];

        while((byt & (1<<Byte.SIZE-bitPos))==0){
            bitPos++;
            if(bitPos==9){
                bytePosition++;
                if(bytePosition>=encodedBitMap.length||bytePosition>=length)
                    return -1; //we've finished
                byt = encodedBitMap[bytePosition];
                bitPos=2;
            }
            highestFound++;
        }
        return highestFound;
    }
}
