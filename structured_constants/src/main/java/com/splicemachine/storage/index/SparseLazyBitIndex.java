package com.splicemachine.storage.index;

/**
 * Lazy implementation of a Sparse Bit Index, which decodes values as needed.
 *
 * @author Scott Fines
 * Created on: 7/8/13
 */
class SparseLazyBitIndex extends LazyBitIndex{
    private int[] offSetAndBitPosition;

    protected SparseLazyBitIndex(byte[] encodedBitMap, int offset, int length) {
        super(encodedBitMap, offset, length);
        offSetAndBitPosition = new int[]{offset,6};

        //check the 0 position
        if((encodedBitMap[offSetAndBitPosition[0]] & 0x08) !=0)
            decodedBits.set(0);
    }

    @Override
    protected int decodeNext() {
        return DeltaCoding.decode(encodedBitMap,offSetAndBitPosition);
    }
}
