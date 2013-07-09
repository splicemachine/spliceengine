package com.splicemachine.storage.index;

import java.util.BitSet;

/**
 * @author Scott Fines
 *         Created on: 7/7/13
 */
public class BitIndexing {

    private BitIndexing(){}

    public static BitIndex uncompressedBitMap(BitSet set){
        return UncompressedBitIndex.create(set);
    }

    public static BitIndex uncompressedBitMap(byte[] bytes, int indexOffset, int indexSize){
        return new UncompressedLazyBitIndex(bytes,indexOffset,indexSize);
    }

    public static BitIndex sparseBitMap(BitSet set){
        return SparseBitIndex.create(set);
    }

    public static BitIndex sparseBitMap(byte[] bytes, int indexOffset, int indexSize){
        return new SparseLazyBitIndex(bytes,indexOffset,indexSize);
    }

    public static BitIndex compressedBitMap(byte[] bytes, int indexOffset, int indexSize){
        return new LazyCompressedBitIndex(bytes,indexOffset,indexSize);
    }

    public static BitIndex compressedBitMap(BitSet set){
        return new DenseCompressedBitIndex(set);
    }
}
