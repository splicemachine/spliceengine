/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.storage.index;

import com.carrotsearch.hppc.BitSet;

/**
 * Main class for constructing BitIndices of proper type.
 *
 * This class is useful as a mechanism for transparently choosing the most efficient implementation
 * of a given requirement (e.g. compressed, uncompressed, sparse, etc.).
 *
 * @author Scott Fines
 * Created on: 7/7/13
 */
public class BitIndexing {

    private BitIndexing(){}

    /**
     * Constructs an Uncompressed, Densely arranged BitIndex from the specified bit set.
     *
     * The resulting BitIndex will encode using 1 bit for each bit up to the largest set bit in {@code set}.
     * For example, if {@code set } looks like 1000 0010 0000 1000, then the returned BitIndex will
     * encode as 1000 0010 0000 1000.
     *
     * This type of index is most suitable for small indices, or indices which have a large degree of
     * switching between set and not-set bits, but it tends to occupy too much space when there is low
     * cardinality (e.g. a small number of set bits), or when data is very skewed (e.g. all set bits towards
     * one end of the set or another).
     *
     * @param set the set to represent.
     * @return an Uncompressed BitIndex
     */
    public static BitIndex uncompressedBitMap(BitSet set,BitSet scalarFields,BitSet floatFields,BitSet doubleFields){
        return UncompressedBitIndex.create(set,scalarFields,floatFields,doubleFields);
    }

    /**
     * Constructs an Uncompressed, Densely arranged BitIndex from the specified buffer, starting
     * at the specified offset and continuing the specified length.
     *
     * The returned BitIndex will have the same encoding characteristics as {@link #uncompressedBitMap(java.util.BitSet)}.
     *
     * Note: If {@code bytes} is <i>not</i> encoded using dense, uncompressed encoding, this index will <em>not</em>
     * throw an error--and incorrect BitIndex <em>will</em> be returned.
     *
     * There is no guarantee that the returned BitIndex is fully populated--it is possible that the implementation
     * returned may elect to defer decoding from the stream until a later time.
     *
     * @param bytes the buffer to read the index from
     * @param indexOffset the offset within the buffer where the index starts
     * @param indexSize the size of the index.
     * @return an Uncompressed, Densely arranged BitIndex with bits set at the same location as 1s in the underlying
     * buffer ({@code bytes}).
     */
    public static BitIndex uncompressedBitMap(byte[] bytes, int indexOffset, int indexSize){
				return UncompressedBitIndex.wrap(bytes,indexOffset,indexSize);
//        return new UncompressedLazyBitIndex(bytes,indexOffset,indexSize);
    }

    /**
     * Constructs a Sparse-encoding BitIndex from the specified BitSet.
     *
     * The returned BitIndex will encode using a <em>sparse</em> representation. In this encoding, the
     * position of the set bit is encoded in a compact format, rather than encoding every bit as in
     * {@link #uncompressedBitMap(java.util.BitSet)}. For example, if {@code set} looks like
     * 0001 0100 0010 0100, then the returned BitIndex will encode 4,6,11,14 (commas are present for clarity,
     * they are not present in the actual encoding).
     *
     * The actual positional numbers are encoded using an Elias Delta Code. For more information, see
     * <a href="http://en.wikipedia.org/wiki/Elias_delta_coding">Wikipedia's article</a>. This encoding
     * has the advantage of encoding numbers using a near-optimal number of bits for each entry. Since this
     * encoding does not generally allow for 0, an additional header bit is used to indicate whether
     * the 0-entry is populated.
     *
     * This type of encoding is primarily useful when the number of set bits is extremely small (perhaps 5% or
     * fewer of the total length of the set). Additionally, because Delta Coding encodes small integers using
     * less space than large numbers, the space used is in some circumstances larger than
     * {@link #compressedBitMap(java.util.BitSet)}.
     *
     * @param set the set to use for the index
     * @return a BitIndex which encodes using a Sparse format.
     */
    public static BitIndex sparseBitMap(BitSet set,BitSet lengthFields){
        throw new UnsupportedOperationException();
//        return SparseBitIndex.create(set,lengthFields);
    }

    public static BitIndex sparseBitMap(BitSet set,BitSet scalarFields,BitSet floatFields, BitSet doubleFields){
        return SparseBitIndex.create(set,scalarFields,floatFields,doubleFields);
    }

    /**
     * Constructs a Sparse-encoding BitIndex from the specified buffer, starting at the specified offset
     * and continuing to the specified length.
     *
     * Note that implementations may elect to defer actual decoding of part (or all) of the buffer until
     * necessary to do so.
     *
     * Note: If {@code bytes} is <i>not</i> encoded using a sparse encoding, this index will <em>not</em>
     * throw an error--an incorrect BitIndex <em>will</em> be returned. This is unavoidable, as there is
     * no practical way to determine whether or not a BitIndex is incorrectly encoded or not.
     *
     * @param bytes the buffer containing the index
     * @param indexOffset the offset at which the index starts
     * @param indexSize the total size of the index
     * @return a Sparsely-encoded BitIndex.
     */
    public static BitIndex sparseBitMap(byte[] bytes, int indexOffset, int indexSize){
				return SparseBitIndex.wrap(bytes,indexOffset,indexSize);
//        return new SparseLazyBitIndex(bytes,indexOffset,indexSize);
    }

    /**
     * Constructs a Dense, Compressed BitIndex from the specified BitSet.
     *
     * The returned implementation encodes the index using Run Length Encoding
     * (see <a href="http://en.wikipedia.org/wiki/Run-length_encoding" /> for more information). In this encoding,
     * consecutive 1s (or zeros) are encoded with a 1 (or 0), followed by a Delta-encoded integer indicating the
     * number of consecutive bit which have the same value. For example, if {@code set} represents
     * 0010 1000 0001 0100, then the compressed encoding will be 021101,110611,011102 (commas are inserted for
     * clarity and are not actually present in the encoding). This allows long sequences of the same value (either
     * set or not set) to be represented using (potentially) fewer bits.
     *
     * The scalar length value is encoded using Elias Delta Coding, which has near-optimal bit length for
     * non-negative integers.
     * This encoding is identical to the encoding used for {@link #sparseBitMap(java.util.BitSet)}, and shares
     * many of the same strengths and flaws.
     *
     * This implementation is generally best suited to either moderately sparse <em>or</em> highly-dense bitmaps
     * which have a clustered distribution (i.e. they tend to clump together in groups).
     * If the distribution is too uniform, then the additional overhead to represent the index
     * overwhelms the savings gained by the length representation
     * (a pathological case is the BitSet 0101 0101 0101 0101, which would encode to
     * 0111 0111 0111 0111 0111 0111 0111 0111, which is twice as large as the uncompressed encoding).
     *
     *
     * @param set the set to encode
     * @return a BitIndex which encodes using a Dense format.
     */
    public static BitIndex compressedBitMap(BitSet set,BitSet lengthDelimitedFields){
        throw new UnsupportedOperationException();
//        return new DenseCompressedBitIndex(set,lengthDelimitedFields);
    }

    public static BitIndex compressedBitMap(BitSet set,BitSet scalarFields,BitSet floatFields, BitSet doubleFields){
        return new DenseCompressedBitIndex(set,scalarFields,floatFields,doubleFields);
    }
    /**
     * Constructs a Dense,Compressed BitIndex from the specified buffer, beginning at {@code indexOffset} and
     * consuming {@code indexSize} bytes.
     *
     * This implementation relies on an encoding identical to {@link #compressedBitMap(java.util.BitSet)}. Because
     * it is, in general, impossible to determine if a given byte-sequence is not encoded properly, no error
     * will be emitted if an improperly-encoded buffer is passed in. An incorrect answer <em>will</em> be returned.
     *
     * The returned implementation reserves the right to decode (either fully or in part) the buffer at any time.
     *
     * @param bytes the buffer containing the index
     * @param indexOffset the start of the index within the buffer
     * @param indexSize the size of the index within the buffer
     * @return a Dense, Compressed BitIndex representation of the given buffer.
     */
    public static BitIndex compressedBitMap(byte[] bytes, int indexOffset, int indexSize){
				return DenseCompressedBitIndex.wrap(bytes,indexOffset,indexSize);
//        return new LazyCompressedBitIndex(bytes,indexOffset,indexSize);
    }

    public static BitIndex getBestIndex(BitSet setCols,BitSet scalarFields,BitSet floatFields, BitSet doubleFields) {
//            if(scalarFields==null)scalarFields = new BitSet(); //default to no length-delimited fields
//            if(floatFields==null)floatFields = new BitSet();
//            if(doubleFields==null)doubleFields= new BitSet();
            BitIndex indexToUse = BitIndexing.uncompressedBitMap(setCols,scalarFields,floatFields,doubleFields);
            //see if we can improve space via compression
            BitIndex denseCompressedBitIndex = BitIndexing.compressedBitMap(setCols,scalarFields,floatFields,doubleFields);
            if(denseCompressedBitIndex.encodedSize() < indexToUse.encodedSize()){
                indexToUse = denseCompressedBitIndex;
            }
            //see if sparse is better
            BitIndex sparseBitMap = BitIndexing.sparseBitMap(setCols,scalarFields,floatFields,doubleFields);
            if(sparseBitMap.encodedSize()<indexToUse.encodedSize()){
                indexToUse = sparseBitMap;
            }
            return indexToUse;
    }

    public static BitIndex wrap(byte[] bytes,int offset,int length){
        byte headerByte = bytes[offset];
        if((headerByte & 0x80) != 0){
            if((headerByte & 0x40)!=0){
                return BitIndexing.compressedBitMap(bytes,offset,length);
            }else{
                return BitIndexing.uncompressedBitMap(bytes,offset,length);
            }
        }else{
            return BitIndexing.sparseBitMap(bytes,offset,length);
        }
    }
}
