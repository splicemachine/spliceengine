/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
package com.splicemachine.access.impl.data;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.shared.common.sanity.SanityManager;
import org.apache.spark.sql.catalyst.expressions.UnsafeRow;
import org.apache.spark.sql.catalyst.expressions.codegen.BufferHolder;
import org.apache.spark.sql.catalyst.expressions.codegen.UnsafeRowWriter;
import org.apache.spark.unsafe.Platform;
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
        calculateBitSetWidthInBytes(numFields) + UnsafeRecord.UNSAFE_INC;  // msirek-temp
    }

    public static int cardinality(Object baseObject, long baseOffset, int bitSetWidthInWords) {

        long addr = baseOffset;
        int sum = 0;
        // msirek-temp->
        long size = ((byte[])baseObject).length;
        for (int i = 0; i < bitSetWidthInWords; i++, addr += 8) {
            if (addr >= size)
                SanityManager.THROWASSERT("Invalid access of byte array");
            // <- msirek-temp
            sum += Long.bitCount(Platform.getLong(baseObject, addr));
        }
        return sum;
    }

    /**
     *
     * BitSet Or Function requiring the source to be larger than the or.
     *
     * @param srcObject
     * @param srcOffset
     * @param srcBitSetWidthInWords
     * @param orObject
     * @param orOffset
     * @param orBitSetWidthInWords
     */
    public static void or(Object srcObject, long srcOffset, int srcBitSetWidthInWords,
                   Object orObject, long orOffset, int orBitSetWidthInWords) {
        assert srcBitSetWidthInWords >= orBitSetWidthInWords:"srcBitSet is smaller than the or bitset, not allowed";
        long srcAddr = srcOffset;
        long orAddr = orOffset;
        for (int k = 0; k < orBitSetWidthInWords; ++k, srcAddr += 8, orAddr += 8) {
            long srcWord = Platform.getLong(srcObject, srcAddr);
            srcWord |= Platform.getLong(orObject, orAddr);
            Platform.putLong(srcObject,srcAddr,srcWord);
        }
    }

    public static String displayBitSet(Object baseObject, long baseOffset, int bitSetWidthInWords) {
        StringBuilder b = new StringBuilder(8*bitSetWidthInWords + 2);
        b.append('{');

        int i = nextSetBit(baseObject,baseOffset,0,bitSetWidthInWords);
        if (i != -1) {
            b.append(i);
            while (true) {
                if (++i < 0) break;
                if ((i = nextSetBit(baseObject,baseOffset,i,bitSetWidthInWords)) < 0) break;
                b.append(", ").append(i);
            }
        }
        b.append('}');
        return b.toString();

    }

    public static int numberOfWordsForColumns(int numberOfColumns) {
        return UnsafeRecordUtils.calculateBitSetWidthInBytes(numberOfColumns)/8;
    }


/*
    public byte[] writeRecord(int[] columns, DataValueDescriptor[] dvds) throws StandardException {
            assert columns.length == dvds.length:"Columns Passed have mismatch with data passed";
            UnsafeRow ur = new UnsafeRow(columns.length);
            BufferHolder bufferHolder = new BufferHolder(ur);
            UnsafeRowWriter writer = new UnsafeRowWriter(bufferHolder,columns.length);
            System.out.println("writer " + writer);
            int bitSetWidth = UnsafeRecordUtils.calculateBitSetWidthInBytes(columns.length);
            System.out.println("bitSetWidth " + bitSetWidth);
            writer.reset();
            int j = 0;
            for (int i = 0; i< columns.length; i++) {
                dvds[i].write(writer,j);
                j++;
            }
            Platform.copyMemory(bufferHolder.buffer,16,baseObject,
                    baseOffset+UNSAFE_INC+bitSetWidth,bufferHolder.cursor);

        }

    }
*/
}
