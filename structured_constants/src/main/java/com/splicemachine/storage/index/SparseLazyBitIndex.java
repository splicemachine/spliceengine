package com.splicemachine.storage.index;

/**
 * Lazy implementation of a Sparse Bit Index, which decodes values as needed.
 *
 * @author Scott Fines
 * Created on: 7/8/13
 */
class SparseLazyBitIndex extends LazyBitIndex{

    protected SparseLazyBitIndex(byte[] encodedBitMap, int offset, int length) {
        super(encodedBitMap, offset, length,5);

        if(bitReader.hasNext()&&bitReader.next()!=0){
            decodedBits.set(0); //check the zero bit
        }
    }

    @Override
    protected int decodeNext() {
        return DeltaCoding.decode(bitReader);
    }
}
