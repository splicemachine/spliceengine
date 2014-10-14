package com.splicemachine.primitives;

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

    public static final ByteComparator BASE_COMPARATOR = new ByteComparator() {
        @Override
        public int compare(byte[] b1, int b1Offset, int b1Length, byte[] b2, int b2Offset, int b2Length) {
            int lLength = b1Length+b1Offset> b1.length? b1.length: b1Length;
            int rLength = b2Length+b2Offset> b2.length? b2.length: b2Length;
            int length = lLength<=rLength? lLength: rLength;
            for (int i = 0; i < length; i++) {
                byte leftByte = b1[b1Offset + i];
                byte rightByte = b2[b2Offset + i];
                if (leftByte < rightByte) {
                    return -1;
                }else if(rightByte>leftByte) return 1;
            }
            return 0;
        }

        @Override
        public boolean equals(byte[] b1, int b1Offset, int b1Length, byte[] b2, int b2Offset, int b2Length) {
            int lLength = b1Length+b1Offset> b1.length? b1.length: b1Length;
            int rLength = b2Length+b2Offset> b2.length? b2.length: b2Length;
            int length = lLength<=rLength? lLength: rLength;
            for (int i = 0; i < length; i++) {
                byte leftByte = b1[b1Offset + i];
                byte rightByte = b2[b2Offset + i];
                if (leftByte != rightByte) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int compare(byte[] o1, byte[] o2) {
            return compare(o1,0,o1.length,o2,0,o2.length);
        }
    };

    /**
     * @return A Simple ByteComparator which performs comparisons using one-byte-at-a-time logic.
     * A more efficient implementation would compare them one word at a time, which is what Hbase does.
     * However, to avoid the Hbase dependency, we don't have that implementation here; it is preferable
     * that you use that instead of this when performance is necessary.
     */
    public static ByteComparator basicByteComparator(){
        return BASE_COMPARATOR;
    }

    /**
     * Convert a long value to a byte array using big-endian.
     *
     * @param val value to convert
     * @return the byte array
     */
    public static byte[] toBytes(long val) {
        byte [] b = new byte[8];
        for (int i = 7; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }

    /**
     * Convert a long value into the specified byte array, using big-endian
     * order.
     *
     * @param x the value to encode
     * @param data the destination byte[]
     * @param offset the offset to place the encoded data
     * @throws java.lang.ArrayIndexOutOfBoundsException if {@code data.length-offset <8}.
     */
    public static void toBytes(long x, byte[] data, int offset){
        data[offset]   = (byte)(x>>56);
        data[offset+1] = (byte)(x>>48);
        data[offset+2] = (byte)(x>>40);
        data[offset+3] = (byte)(x>>32);
        data[offset+4] = (byte)(x>>24);
        data[offset+5] = (byte)(x>>16);
        data[offset+6] = (byte)(x>>8);
        data[offset+7] = (byte)(x   );
    }

    /**
     * Convert an int value to a byte array
     * @param val value
     * @return the byte array
     */
    public static byte[] toBytes(int val) {
        byte [] b = new byte[4];
        for(int i = 3; i > 0; i--) {
            b[i] = (byte) val;
            val >>>= 8;
        }
        b[0] = (byte) val;
        return b;
    }


    public static int toInt(byte[] bytes){
        return toInt(bytes,0);
    }
    /**
     * Converts a byte array to an int value
     * @param bytes byte array
     * @param offset offset into array
     * @return the int value
     */
    public static int toInt(byte[] bytes, int offset) {
        int n = 0;
        for(int i = offset; i < (offset + 4); i++) {
            n <<= 8;
            n ^= bytes[i] & 0xFF;
        }
        return n;
    }

    public static long toLong(byte[] bytes){
        return toLong(bytes,0);
    }

    /**
     * Converts a byte array to a long value.
     *
     * @param bytes array of bytes
     * @param offset offset into array
     * @return the long value
     */
    public static long toLong(byte[] bytes, int offset) {
        long l = 0;
        for(int i = offset; i < offset + 8; i++) {
            l <<= 8;
            l ^= bytes[i] & 0xFF;
        }
        return l;
    }

    /**
     * Converts a byte array to a short value
     * @param bytes byte array
     * @return the short value
     */
    public static short toShort(byte[] bytes) {
        return toShort(bytes, 0);
    }

    /**
     * Converts a byte array to a short value
     * @param bytes byte array
     * @param offset offset into array
     * @return the short value
     * or if there's not enough room in the array at the offset indicated.
     */
    public static short toShort(byte[] bytes, int offset) {
        short n = 0;
        n ^= bytes[offset] & 0xFF;
        n <<= 8;
        n ^= bytes[offset+1] & 0xFF;
        return n;
    }

}
