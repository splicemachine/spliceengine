package com.splicemachine.utils;

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
