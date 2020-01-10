/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.primitives;


import com.splicemachine.utils.Pair;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import com.carrotsearch.hppc.BitSet;

/**
 * Utility class which encompasses basic conversion logic.
 *
 * A lot of this logic is contained in HBase's Bytes class,
 * but this is presented separately to avoid adding an HBase dependency
 *
 * @author Scott Fines
 * Date: 8/5/14
 */
public class Bytes {
    public static final String UTF8_ENCODING = "UTF-8";
    public static final Charset UTF8_CHARSET = Charset.forName(UTF8_ENCODING);
    public static final byte[] EMPTY_BYTE_ARRAY = new byte[0];

    /**
     * Size of boolean in bytes
     */
    public static final int SIZEOF_BOOLEAN = Byte.SIZE / Byte.SIZE;

    /**
     * Size of byte in bytes
     */
    public static final int SIZEOF_BYTE = SIZEOF_BOOLEAN;

    /**
     * Size of char in bytes
     */
    public static final int SIZEOF_CHAR = Character.SIZE / Byte.SIZE;

    /**
     * Size of double in bytes
     */
    public static final int SIZEOF_DOUBLE = Double.SIZE / Byte.SIZE;

    /**
     * Size of float in bytes
     */
    public static final int SIZEOF_FLOAT = Float.SIZE / Byte.SIZE;

    /**
     * Size of int in bytes
     */
    public static final int SIZEOF_INT = Integer.SIZE / Byte.SIZE;

    /**
     * Size of long in bytes
     */
    public static final int SIZEOF_LONG = Long.SIZE / Byte.SIZE;

    /**
     * Size of short in bytes
     */
    public static final int SIZEOF_SHORT = Short.SIZE / Byte.SIZE;


    /**
     * Lexicographical byte[] comparator that places empty byte[] values before non-empty values.
     */
    public static final Comparator<byte[]> startComparator = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] o1, byte[] o2) {
            return compareBytes(false, o1, o2);
        }
    };

    /**
     * Lexicographical byte[] comparator that places empty byte[] values after non-empty values.
     */
    public static final Comparator<byte[]> endComparator = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] o1, byte[] o2) {
            return compareBytes(true, o1, o2);
        }
    };

    /**
     * Parameterized lexicographical byte[] comparison.
     *
     * @param emptyGreater indicates whether empty byte[] are greater or less than non-empty values.
     */
    public static int compareBytes(boolean emptyGreater, byte[] x, byte[] y) {
        if (empty(x)) {
            if (empty(y)) {
                return 0;
            } else {
                return emptyGreater ? 1 : -1;
            }
        } else if (empty(y)) {
            return emptyGreater ? -1 : 1;
        } else {
            return BASE_COMPARATOR.compare(x, y);
        }
    }

    public static boolean empty(byte[] x) {
        return x == null || BASE_COMPARATOR.compare(x, EMPTY_BYTE_ARRAY) == 0;
    }


    public static final ByteComparator BASE_COMPARATOR = new ByteComparator() {
        @Override
        public int compare(byte[] b1, int b1Offset, int b1Length, byte[] b2, int b2Offset, int b2Length) {
            if (b1 == b2 && b1Offset == b2Offset && b1Length == b2Length) {
                return 0;
            } else {
                int end1 = b1Offset + b1Length;
                int end2 = b2Offset + b2Length;
                int i = b1Offset;

                for (int j = b2Offset; i < end1 && j < end2; ++j) {
                    int a = b1[i] & 255;
                    int b = b2[j] & 255;
                    if (a != b) {
                        return a - b;
                    }

                    ++i;
                }

                return b1Length - b2Length;
            }
        }

        @Override
        public int compare(ByteBuffer buffer, byte[] b2, int b2Offset, int b2Length) {
            buffer.mark();
            try {
                int b1Length = buffer.remaining();
                int length = b1Length <= b2Length ? b1Length : b2Length;
                for (int i = 0; i < length; i++) {
                    int leftByte = buffer.get() & 0xff;
                    int rightByte = b2[b2Offset + i] & 0xff;
                    if (leftByte != rightByte) return leftByte - rightByte;
                }
                return b1Length - b2Length;
            } finally {
                buffer.reset();
            }
        }

        @Override
        public int compare(ByteBuffer lBuffer, ByteBuffer rBuffer) {
            if (lBuffer == rBuffer) return 0;
            lBuffer.mark();
            rBuffer.mark();
            try {
                int lLength = lBuffer.remaining();
                int rLength = rBuffer.remaining();
                int length = lLength <= rLength ? lLength : rLength;
                for (int i = 0; i < length; i++) {
                    int leftByte = lBuffer.get() & 0xff;
                    int rightByte = rBuffer.get() & 0xff;
                    if (leftByte < rightByte) {
                        return -1;
                    } else if (rightByte < leftByte) return 1;
                }
                return lLength - rLength;
            } finally {
                lBuffer.reset();
                rBuffer.reset();
            }
        }

        @Override
        public boolean equals(byte[] b1, int b1Offset, int b1Length, byte[] b2, int b2Offset, int b2Length) {
            return compare(b1, b1Offset, b1Length, b2, b2Offset, b2Length) == 0;
        }

        @Override
        public boolean equals(byte[] b1, byte[] b2) {
            return equals(b1, 0, b1.length, b2, 0, b2.length);
        }

        @Override
        public boolean equals(ByteBuffer buffer, byte[] b2, int b2Offset, int b2Length) {
            return compare(buffer, b2, b2Offset, b2Length) == 0;
        }

        @Override
        public boolean equals(ByteBuffer lBuffer, ByteBuffer rBuffer) {
            return compare(lBuffer, rBuffer) == 0;
        }

        @Override
        public int compare(byte[] o1, byte[] o2) {
            return compare(o1, 0, o1.length, o2, 0, o2.length);
        }

        @Override
        public boolean isEmpty(byte[] stop) {
            return stop == null || stop.length == 0;
        }
    };
    public static boolean isLittleEndian = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

    /**
     * @return A Simple ByteComparator which performs comparisons using one-byte-at-a-time logic.
     * A more efficient implementation would compare them one word at a time, which is what Hbase does.
     * However, to avoid the Hbase dependency, we don't have that implementation here; it is preferable
     * that you use that instead of this when performance is necessary.
     */
    public static ByteComparator basicByteComparator() {
        return BASE_COMPARATOR;
    }

    /**
     * Convert a long value to a byte array using the native byte order(usually big-endian).
     *
     * @param val value to convert
     * @return the byte array
     */
    public static byte[] toBytes(long val) {
        return BigEndianBits.toBytes(val);
    }

    /**
     * Convert a long value into the specified byte array, using big-endian
     * order.
     *
     * @param x      the value to encode
     * @param data   the destination byte[]
     * @param offset the offset to place the encoded data
     * @throws java.lang.ArrayIndexOutOfBoundsException if {@code data.length-offset <8}.
     */
    public static void toBytes(long x, byte[] data, int offset) {
        BigEndianBits.toBytes(x, data, offset);
    }

    /**
     * Convert an int value to a byte array, using the native byte order of the platform
     *
     * @param val value
     * @return the byte array
     */
    public static byte[] toBytes(int val) {
        return BigEndianBits.toBytes(val);
    }

    /**
     * Convert a long value into the specified byte array, using the native byte order of the platform
     *
     * @param x      the value to encode
     * @param data   the destination byte[]
     * @param offset the offset to place the encoded data
     * @throws java.lang.ArrayIndexOutOfBoundsException if {@code data.length-offset <8}.
     */
    public static void toBytes(int x, byte[] data, int offset) {
        BigEndianBits.toBytes(x, data, offset);
    }


    public static int toInt(byte[] bytes) {
        return toInt(bytes, 0);
    }

    public static byte[] toBytes(float f){
        return toBytes(Float.floatToRawIntBits(f));
    }
    /**
     * Converts a byte array to an int value, using the platform byte order
     *
     * @param bytes  byte array
     * @param offset offset into array
     * @return the int value
     */
    public static int toInt(byte[] bytes, int offset) {
        return BigEndianBits.toInt(bytes, offset);
    }

    /**
     * Converts a byte array to a short value, using the byte order of the platform
     *
     * @param bytes byte array
     * @return the short value
     */
    public static short toShort(byte[] bytes) {
        return toShort(bytes, 0);
    }

    /**
     * Converts a byte array to a short value, using the sort order of the platform
     *
     * @param bytes  byte array
     * @param offset offset into array
     * @return the short value
     * or if there's not enough room in the array at the offset indicated.
     */
    public static short toShort(byte[] bytes, int offset) {
        return BigEndianBits.toShort(bytes, offset);
    }

    public static byte[] prepend(byte element, byte[] existing) {
        byte[] newBytes = new byte[existing.length + 1];
        newBytes[0] = element;
        System.arraycopy(existing, 0, newBytes, 1, existing.length);
        return newBytes;
    }

    public static byte[] concat(Collection<byte[]> elements) {
        int length = 0;
        for (byte[] b : elements) {
            length += b.length;
        }
        byte[] result = new byte[length];
        int position = 0;
        for (byte[] b : elements) {
            System.arraycopy(b, 0, result, position, b.length);
            position += b.length;
        }
        return result;
    }

    public static void unsignedIncrement(byte[] array, int index) {
        if (array.length <= 0) return; //nothing to do
        while (index >= 0) {
            int value = array[index] & 0xff;
            if (value == 255) {
                array[index] = 0;
                //we've looped past the entry, so increment the next byte in the array
                index--;
            } else {
                array[index]++;
                return;
            }
        }
        throw new AssertionError("Unable to increment byte[] " + Arrays.toString(array) + ", incrementing would violate sort order");
    }

    public static void unsignedDecrement(byte[] array, int index) {
        while (index >= 0) {
            int value = array[index] & 0xff;
            if (value == 0) {
                array[index] = (byte) 0xff;
                index--;
            } else {
                array[index]--;
                return;
            }
        }
        throw new AssertionError("Unable to decrement " + Arrays.toString(array) + ", as it would violate sort-order");
    }

    /**
     * Converts a string to a UTF-8 byte array.
     *
     * @param s string
     * @return the byte array
     */
    public static byte[] toBytes(String s) {
        return s.getBytes(UTF8_CHARSET);
    }

    /**
     * Convert a boolean to a byte array. True becomes -1
     * and false becomes 0.
     *
     * @param b value
     * @return <code>b</code> encoded in a byte array.
     */
    public static byte[] toBytes(final boolean b) {
        return new byte[]{b ? (byte) -1 : (byte) 0};
    }


    /**
     * Reverses {@link #toBytes(boolean)}
     *
     * @param b array
     * @return True or false.
     */
    public static boolean toBoolean(final byte[] b) {
        if (b.length != 1) {
            throw new IllegalArgumentException("Array has wrong size: " + b.length);
        }
        return b[0] != (byte) 0;
    }

    /**
     * @return a [start, end) pair identifying the ranges of values that are in both [start1, end1) and [start2, end2)
     * under lexicographic sorting of values.
     */
    public static Pair<byte[], byte[]> intersect(byte[] start1, byte[] end1, byte[] start2, byte[] end2) {
        if (overlap(start1, end1, start2, end2)) {
            return Pair.newPair(
                    max(startComparator, start1, start2),
                    min(endComparator, end1, end2));
        } else {
            return null;
        }
    }

    /**
     * @return whether [start1, end1) and [start2, end2) share any values
     */
    public static boolean overlap(byte[] start1, byte[] end1, byte[] start2, byte[] end2) {
        return startLessThanEnd(start1, end2) && startLessThanEnd(start2, end1);
    }

    /**
     * @return whether the given start range value is less than the end range value considering lexicographical ordering
     * and treating empty byte[] as -infinity in starting positions and infinity in ending positions
     */
    private static boolean startLessThanEnd(byte[] start1, byte[] end2) {
        return (empty(end2) || empty(start1) || BASE_COMPARATOR.compare(start1, end2) < 0);
    }

    /**
     * @return the value that sorts lowest.
     */
    public static byte[] min(Comparator<byte[]> comparator, byte[] x, byte[] y) {
        return (comparator.compare(y, x) <= 0) ? y : x;
    }

    /**
     * @return the value that sorts highest.
     */
    public static byte[] max(Comparator<byte[]> comparator, byte[] x, byte[] y) {
        return (comparator.compare(x, y) <= 0) ? y : x;
    }

    private static final char[] hexArray = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    public static String toHex(byte[] bytes) {
        return toHex(bytes, 0, bytes.length);
    }

    public static String toHex(byte[] bytes, int offset, int length) {
        if (bytes == null || length <= 0) return "";
        char[] hexChars = new char[length * 2];
        int v;
        for (int j = 0, k = offset; j < length; k++, j++) {
            v = bytes[k] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static byte[] fromHex(String line) {
        if (line == null || line.length() <= 0) return null;
        char[] hexChars = line.toCharArray();
        byte[] data = new byte[hexChars.length / 2];
        for (int i = 0, pos = 0; i < hexChars.length - 1; i += 2, pos++) {
            char n1 = hexChars[i];
            char n2 = hexChars[i + 1];
            data[pos] = (byte) ((Bytes.toBinaryFromHex((byte) n1) << 4) + Bytes.toBinaryFromHex((byte) n2));
        }
        return data;
    }

    public static String toHex(ByteBuffer bytes) {
        if (bytes == null || bytes.remaining() <= 0) return "";
        byte[] bits = new byte[bytes.remaining()];
        bytes.get(bits);

        return toHex(bits);
    }

    /**
     * Takes a ASCII digit in the range A-F0-9 and returns
     * the corresponding integer/ordinal value.
     *
     * @param ch The hex digit.
     * @return The converted hex value as a byte.
     */
    public static byte toBinaryFromHex(byte ch) {
        if (ch >= 'A' && ch <= 'F')
            return (byte) ((byte) 10 + (byte) (ch - 'A'));
        // else
        return (byte) (ch - '0');
    }

    public static byte[] unsignedCopyAndIncrement(byte[] start) {
        byte[] next = new byte[start.length];
        System.arraycopy(start, 0, next, 0, next.length);
        unsignedIncrement(next, next.length - 1);
        return next;
    }

    public static boolean isRowInRange(byte[] row,byte[] start, byte[] stopKey){
        return startComparator.compare(row,start)>=0 && endComparator.compare(row,stopKey)<0;
    }

    public static void intToBytes(int value,byte[] data,int offset) {
        data[offset] = (byte)(value >>> 24);
        data[offset+1] = (byte)(value >>> 16);
        data[offset+2] = (byte)(value >>> 8);
        data[offset+3] = (byte)(value);
    }

    public static void longToBytes(long x, byte[] data, int offset){
        data[offset] = (byte) (x >> 56);
        data[offset + 1] = (byte) (x >> 48);
        data[offset + 2] = (byte) (x >> 40);
        data[offset + 3] = (byte) (x >> 32);
        data[offset + 4] = (byte) (x >> 24);
        data[offset + 5] = (byte) (x >> 16);
        data[offset + 6] = (byte) (x >> 8);
        data[offset+7] = (byte)(x   );
    }
    public static int bytesToInt(byte[] data, int offset) {
        int value = 0;
        value |= (data[offset] & 0xff)<<24;
        value |= (data[offset+1] & 0xff)<<16;
        value |= (data[offset+2] & 0xff)<< 8;
        value |= (data[offset+3] & 0xff);
        return value;
    }

    public static long bytesToLong(byte[] data, int offset){
        long value = 0;
        value |= (data[offset] & 0xffL)<<56;
        value |= (data[offset+1] & 0xffL)<<48;
        value |= (data[offset+2] & 0xffL)<<40;
        value |= (data[offset+3] & 0xffL)<<32;
        value |= (data[offset+4] & 0xffL)<<24;
        value |= (data[offset+5] & 0xffL)<<16;
        value |= (data[offset+6] & 0xffL)<<8;
        value |= (data[offset+7] & 0xffL);
        return value;
    }

    public static byte[] toByteArray(BitSet bits) {
        byte[] bytes = new byte[ (int) ((bits.length()+7)/8+4)];
        intToBytes((int)(bits.length()+7)/8,bytes,0);
        for (int i=0; i<bits.length(); i++) {
            if (bits.get(i)) {
                bytes[(bytes.length-4)-i/8-1+4] |= 1<<(i%8);
            }
        }
        return bytes;
    }

    public static Pair<BitSet,Integer> fromByteArray(byte[] data, int offset){
        int numBytes = bytesToInt(data,offset);
        BitSet bitSet = new BitSet();
        int currentPos=0;
        for(int i=numBytes+offset+4-1;i>=offset+4;i--){
            byte byt = data[i];
            //a 1 in the position indicates the field is set
            for(int pos=0;pos<8;pos++){
                if((byt & (1<<pos))>0)
                    bitSet.set(currentPos);
                currentPos++;
            }
        }
        return Pair.newPair(bitSet,offset+numBytes+4);
    }

    public static byte[] slice(byte[] data, int offset, int length) {
        byte[] slice = new byte[length];
        System.arraycopy(data,offset,slice,0,length);
        return slice;
    }

    public static byte[] longToBytes(long n) {
        byte[] data = new byte[8];
        longToBytes(n,data,0);
        return data;
    }

    public static boolean equals(final byte[] left, int leftOffset, int leftLen,
                                 final byte[] right, int rightOffset, int rightLen) {
        // short circuit case
        if (left == right &&
                leftOffset == rightOffset &&
                leftLen == rightLen) {
            return true;
        }
        // different lengths fast check
        if (leftLen != rightLen) {
            return false;
        }
        if (leftLen == 0) {
            return true;
        }

        // Since we're often comparing adjacent sorted data,
        // it's usual to have equal arrays except for the very last byte
        // so check that first
        if (left[leftOffset + leftLen - 1] != right[rightOffset + rightLen - 1]) return false;

        return BASE_COMPARATOR.
                compare(left, leftOffset, leftLen, right, rightOffset, rightLen) == 0;
    }

    public static byte [] toBytesBinary(String in) {
        // this may be bigger than we need, but let's be safe.
        byte [] b = new byte[in.length()];
        int size = 0;
        for (int i = 0; i < in.length(); ++i) {
            char ch = in.charAt(i);
            if (ch == '\\' && in.length() > i+1 && in.charAt(i+1) == 'x') {
                // ok, take next 2 hex digits.
                char hd1 = in.charAt(i+2);
                char hd2 = in.charAt(i+3);

                // they need to be A-F0-9:
                if (!isHexDigit(hd1) ||
                        !isHexDigit(hd2)) {
                    // bogus escape code, ignore:
                    continue;
                }
                // turn hex ASCII digit -> number
                byte d = (byte) ((toBinaryFromHex((byte)hd1) << 4) + toBinaryFromHex((byte)hd2));

                b[size++] = d;
                i += 3; // skip 3
            } else {
                b[size++] = (byte) ch;
            }
        }
        // resize:
        byte [] b2 = new byte[size];
        System.arraycopy(b, 0, b2, 0, size);
        return b2;
    }

    private static boolean isHexDigit(char c) {
        return
                (c >= 'A' && c <= 'F') ||
                        (c >= '0' && c <= '9');
    }

    /**
     * Converts a byte array to a long value. Assumes there will be
     * {@link #SIZEOF_LONG} bytes available.
     *
     * @param bytes bytes
     * @param offset offset
     * @return the long value
     */
    public static long toLong(byte[] bytes, int offset) {
        return toLong(bytes, offset, SIZEOF_LONG);
    }

    /**
     * Converts a byte array to a long value.
     *
     * @param bytes array of bytes
     * @param offset offset into array
     * @param length length of data (must be {@link #SIZEOF_LONG})
     * @return the long value
     * @throws IllegalArgumentException if length is not {@link #SIZEOF_LONG} or
     * if there's not enough room in the array at the offset indicated.
     */
    public static long toLong(byte[] bytes, int offset, final int length) {
        if (length != SIZEOF_LONG || offset + length > bytes.length) {
            throw explainWrongLengthOrOffset(bytes, offset, length, SIZEOF_LONG);
        }
            long l = 0;
            for(int i = offset; i < offset + length; i++) {
                l <<= 8;
                l ^= bytes[i] & 0xFF;
            }
            return l;
    }

    private static IllegalArgumentException
    explainWrongLengthOrOffset(final byte[] bytes,
                               final int offset,
                               final int length,
                               final int expectedLength) {
        String reason;
        if (length != expectedLength) {
            reason = "Wrong length: " + length + ", expected " + expectedLength;
        } else {
            reason = "offset (" + offset + ") + length (" + length + ") exceed the"
                    + " capacity of the array: " + bytes.length;
        }
        return new IllegalArgumentException(reason);
    }

    /**
     * Converts a byte array to a long value. Reverses
     * {@link #toBytes(long)}
     * @param bytes array
     * @return the long value
     */
    public static long toLong(byte[] bytes) {
        return toLong(bytes, 0, SIZEOF_LONG);
    }



    /**
     * @param left left operand
     * @param right right operand
     * @return True if equal
     */
    public static boolean equals(final byte [] left, final byte [] right) {
        // Could use Arrays.equals?
        //noinspection SimplifiableConditionalExpression
        if (left == right) return true;
        if (left == null || right == null) return false;
        if (left.length != right.length) return false;
        if (left.length == 0) return true;

        // Since we're often comparing adjacent sorted data,
        // it's usual to have equal arrays except for the very last byte
        // so check that first
        if (left[left.length - 1] != right[right.length - 1]) return false;

        return BASE_COMPARATOR.compare(left, right) == 0;
    }

    /**
     * @param a left operand
     * @param buf right operand
     * @return True if equal
     */
    public static boolean equals(byte[] a, ByteBuffer buf) {
        if (a == null) return buf == null;
        if (buf == null) return false;
        if (a.length != buf.remaining()) return false;

        // Thou shalt not modify the original byte buffer in what should be read only operations.
        ByteBuffer b = buf.duplicate();
        for (byte anA : a) {
            if (anA != b.get()) {
                return false;
            }
        }
        return true;
    }

    /**
     * Write a printable representation of a byte array.
     *
     * @param b byte array
     * @return string
     * @see #toStringBinary(byte[], int, int)
     */
    public static String toStringBinary(final byte [] b) {
        if (b == null)
            return "null";
        return toStringBinary(b, 0, b.length);
    }

    /**
     * Converts the given byte buffer to a printable representation,
     * from the index 0 (inclusive) to the limit (exclusive),
     * regardless of the current position.
     * The position and the other index parameters are not changed.
     *
     * @param buf a byte buffer
     * @return a string representation of the buffer's binary contents
     * @see #toBytes(ByteBuffer)
     * @see #getBytes(ByteBuffer)
     */
    public static String toStringBinary(ByteBuffer buf) {
        if (buf == null)
            return "null";
        if (buf.hasArray()) {
            return toStringBinary(buf.array(), buf.arrayOffset(), buf.limit());
        }
        return toStringBinary(toBytes(buf));
    }

    /**
     * Write a printable representation of a byte array. Non-printable
     * characters are hex escaped in the format \\x%02X, eg:
     * \x00 \x05 etc
     *
     * @param b array to write out
     * @param off offset to start at
     * @param len length to write
     * @return string output
     */
    public static String toStringBinary(final byte [] b, int off, int len) {
        StringBuilder result = new StringBuilder();
        // Just in case we are passed a 'len' that is > buffer length...
        if (off >= b.length) return result.toString();
        if (off + len > b.length) len = b.length - off;
        for (int i = off; i < off + len ; ++i ) {
            int ch = b[i] & 0xFF;
            if ( (ch >= '0' && ch <= '9')
                    || (ch >= 'A' && ch <= 'Z')
                    || (ch >= 'a' && ch <= 'z')
                    || " `~!@#$%^&*()-_=+[]{}|;:'\",.<>/?".indexOf(ch) >= 0 ) {
                result.append((char)ch);
            } else {
                result.append(String.format("\\x%02X", ch));
            }
        }
        return result.toString();
    }

    /**
     * Returns a new byte array, copied from the given {@code buf},
     * from the index 0 (inclusive) to the limit (exclusive),
     * regardless of the current position.
     * The position and the other index parameters are not changed.
     *
     * @param buf a byte buffer
     * @return the byte array
     * @see #getBytes(ByteBuffer)
     */
    public static byte[] toBytes(ByteBuffer buf) {
        ByteBuffer dup = buf.duplicate();
        dup.position(0);
        return readBytes(dup);
    }

    private static byte[] readBytes(ByteBuffer buf) {
        byte [] result = new byte[buf.remaining()];
        buf.get(result);
        return result;
    }


    /**
     * @param b Presumed UTF-8 encoded byte array.
     * @return String made from <code>b</code>
     */
    public static String toString(final byte [] b) {
        if (b == null) {
            return null;
        }
        return toString(b, 0, b.length);
    }

    /**
     * Joins two byte arrays together using a separator.
     * @param b1 The first byte array.
     * @param sep The separator to use.
     * @param b2 The second byte array.
     */
    public static String toString(final byte [] b1,
                                  String sep,
                                  final byte [] b2) {
        return toString(b1, 0, b1.length) + sep + toString(b2, 0, b2.length);
    }

    /**
     * This method will convert utf8 encoded bytes into a string. If
     * the given byte array is null, this method will return null.
     *
     * @param b Presumed UTF-8 encoded byte array.
     * @param off offset into array
     * @return String made from <code>b</code> or null
     */
    public static String toString(final byte [] b, int off) {
        if (b == null) {
            return null;
        }
        int len = b.length - off;
        if (len <= 0) {
            return "";
        }
        return new String(b, off, len, UTF8_CHARSET);
    }

    /**
     * This method will convert utf8 encoded bytes into a string. If
     * the given byte array is null, this method will return null.
     *
     * @param b Presumed UTF-8 encoded byte array.
     * @param off offset into array
     * @param len length of utf-8 sequence
     * @return String made from <code>b</code> or null
     */
    public static String toString(final byte [] b, int off, int len) {
        if (b == null) {
            return null;
        }
        if (len == 0) {
            return "";
        }
        return new String(b, off, len, UTF8_CHARSET);
    }

    /**
     * Reverses {@link Bytes.toBytes(boolean)}
     * @param b array
     * @return True or false.
     */
    public static boolean toBoolean(final byte [] b,int offset) {
        return b[offset] != (byte) 0;
    }

    /**
     * @param left left operand
     * @param right right operand
     * @return 0 if equal, &lt; 0 if left is less than right, etc.
     */
    public static int compareTo(byte[] left, byte[] right) {
        return compareBytes(false, left, right);
    }
}
