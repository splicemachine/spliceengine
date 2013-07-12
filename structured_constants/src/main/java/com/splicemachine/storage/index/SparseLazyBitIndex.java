package com.splicemachine.storage.index;

import java.util.BitSet;

/**
 * Lazy implementation of a Sparse Bit Index, which decodes values as needed.
 *
 * @author Scott Fines
 * Created on: 7/8/13
 */
class SparseLazyBitIndex extends LazyBitIndex{

    int lastPos=0;
    protected SparseLazyBitIndex(byte[] encodedBitMap, int offset, int length) {
        super(encodedBitMap, offset, length,5);

        if(bitReader.hasNext()&&bitReader.next()!=0){
            decodedBits.set(0); //check the zero bit
            if(bitReader.hasNext()&&bitReader.next()!=0){
                decodedLengthDelimitedFields.set(0);
            }
        }
    }


    @Override
    protected int decodeNext() {
        int val = DeltaCoding.decode(bitReader);
        if(val<0)
            return val;

        int pos = lastPos+val;
        if(bitReader.hasNext()&&bitReader.next()!=0)
            decodedLengthDelimitedFields.set(pos);
        lastPos=pos;
        return pos;
    }
}
