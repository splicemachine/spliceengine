package com.splicemachine.storage.index;

/**
 * Lazy implementation of an Uncompressed, Dense BitIndex.
 * @author Scott Fines
 * Created on: 7/8/13
 */
class UncompressedLazyBitIndex extends LazyBitIndex{

    private int bytePosition;
    private int bitPos = 5;

    int count = -1;
    protected UncompressedLazyBitIndex(byte[] encodedBitMap,
                                       int offset, int length) {
        super(encodedBitMap, offset, length,5);
        this.bytePosition = offset;

    }

    @Override
    protected int decodeNext() {
        do{
            if(!bitReader.hasNext()) return -1;
            count++;
        }while(bitReader.next()==0);

        return count; //can fit 4 bits in the header byte
    }
}
