package com.splicemachine.storage.index;

import java.util.BitSet;

/**
 * @author Scott Fines
 *         Created on: 7/7/13
 */
public class BitIndexing {


    public static BitIndex uncompressedBitMap(BitSet set){
        return UncompressedBitIndex.create(set);
    }

    public static BitIndex uncompressedBitMap(byte[] bytes, int indexOffset, int indexSize){
        //TODO -sf- construct a more intelligent wrap which does not deserialize the entire
        //structure
        return UncompressedBitIndex.wrap(bytes,indexOffset,indexSize);
    }
}
