package com.splicemachine.access.impl.data;

import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.unsafe.bitset.BitSetMethods;

/**
 *
 *
 */
public class UnsafeRecordUtils {

    private UnsafeRecordUtils() {
        // all methods are designed to be static manipulation
    }

    public static void set(Object baseObject, long baseOffset, int index) {
        BitSetMethods.set(baseObject,baseOffset,index);
    }

    /**
     * Sets the bit at the specified index to {@code false}.
     */
    public static void unset(Object baseObject, long baseOffset, int index) {
        BitSetMethods.unset(baseObject,baseOffset,index);
    }

    /**
     * Returns {@code true} if the bit is set at the specified index.
     */
    public static boolean isSet(Object baseObject, long baseOffset, int index) {
        return BitSetMethods.isSet(baseObject,baseOffset,index);
    }

    /**
     * Returns {@code true} if any bit is set.
     */
    public static boolean anySet(Object baseObject, long baseOffset, long bitSetWidthInWords) {
        return BitSetMethods.anySet(baseObject,baseOffset,bitSetWidthInWords);
    }

    /**
     * Returns the index of the first bit that is set to true that occurs on or after the
     * specified starting index. If no such bit exists then {@code -1} is returned.
     * <p>
     * To iterate over the true bits in a BitSet, use the following loop:
     * <pre>
     * <code>
     *  for (long i = bs.nextSetBit(0, sizeInWords); i &gt;= 0;
     *    i = bs.nextSetBit(i + 1, sizeInWords)) {
     *    // operate on index i here
     *  }
     * </code>
     * </pre>
     *
     * @param fromIndex the index to start checking from (inclusive)
     * @param bitsetSizeInWords the size of the bitset, measured in 8-byte words
     * @return the index of the next set bit, or -1 if there is no such bit
     */
    public static int nextSetBit(
            Object baseObject,
            long baseOffset,
            int fromIndex,
            int bitsetSizeInWords) {
        return BitSetMethods.nextSetBit(baseObject,baseOffset,fromIndex,bitsetSizeInWords);
    }

    public static int calculateBitSetWidthInBytes(int numFields) {
        return UnsafeRow.calculateBitSetWidthInBytes(numFields);
    }

    public static int calculateFixedRecordSize(int numFields) {
        return UnsafeRow.calculateFixedPortionByteSize(numFields)+
        calculateBitSetWidthInBytes(numFields);
    }

}
