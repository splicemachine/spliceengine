package com.splicemachine.primitives;


import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.Collection;

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
                }else if(rightByte<leftByte) return 1;
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

        @Override
        public boolean isEmpty(byte[] stop) {
            return stop==null || stop.length==0;
        }
    };
    public static boolean isLittleEndian = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

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
     * @param x the value to encode
     * @param data the destination byte[]
     * @param offset the offset to place the encoded data
     * @throws java.lang.ArrayIndexOutOfBoundsException if {@code data.length-offset <8}.
     */
    public static void toBytes(long x, byte[] data, int offset){
        BigEndianBits.toBytes(x, data, offset);
    }

    /**
     * Convert an int value to a byte array, using the native byte order of the platform
     * @param val value
     * @return the byte array
     */
    public static byte[] toBytes(int val) {
        return BigEndianBits.toBytes(val);
    }

    /**
     * Convert a long value into the specified byte array, using the native byte order of the platform
     *
     * @param x the value to encode
     * @param data the destination byte[]
     * @param offset the offset to place the encoded data
     * @throws java.lang.ArrayIndexOutOfBoundsException if {@code data.length-offset <8}.
     */
    public static void toBytes(int x, byte[] data, int offset){
        BigEndianBits.toBytes(x,data,offset);
    }


    public static int toInt(byte[] bytes){
        return toInt(bytes,0);
    }
    /**
     * Converts a byte array to an int value, using the platform byte order
     * @param bytes byte array
     * @param offset offset into array
     * @return the int value
     */
    public static int toInt(byte[] bytes, int offset) {
        return BigEndianBits.toInt(bytes,offset);
    }

    public static long toLong(byte[] bytes){
        return toLong(bytes,0);
    }

    /**
     * Converts a byte array to a long value, using the platform byte order
     *
     * @param bytes array of bytes
     * @param offset offset into array
     * @return the long value
     */
    public static long toLong(byte[] bytes, int offset) {
        return BigEndianBits.toLong(bytes, offset);
    }

    /**
     * Converts a byte array to a short value, using the byte order of the platform
     * @param bytes byte array
     * @return the short value
     */
    public static short toShort(byte[] bytes) {
        return toShort(bytes, 0);
    }

    /**
     * Converts a byte array to a short value, using the sort order of the platform
     * @param bytes byte array
     * @param offset offset into array
     * @return the short value
     * or if there's not enough room in the array at the offset indicated.
     */
    public static short toShort(byte[] bytes, int offset) {
        return BigEndianBits.toShort(bytes,offset);
    }

    public static byte[] prepend(byte element, byte[] existing){
        byte[] newBytes = new byte[existing.length+1];
        newBytes[0] = element;
        System.arraycopy(existing,0,newBytes,1,existing.length);
        return newBytes;
    }

    public static byte[] concatenate(Collection<byte[]> elements){
        int length = 0;
        for(byte[] b:elements){
            length+=b.length;
        }
        byte[] result = new byte[length];
        int position =0;
        for(byte[] b:elements){
            System.arraycopy(b,0,result,position,b.length);
            position+=b.length;
        }
        return result;
    }

    public static void unsignedIncrement(byte[] array, int index){
        if(array.length<=0) return; //nothing to do
        while(index>=0){
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
        throw new AssertionError("Unable to increment byte[] "+ Arrays.toString(array) +", incrementing would violate sort order");
    }

    public static void unsignedDecrement(byte[] array, int index){
        while(index>=0){
            int value = array[index] & 0xff;
            if(value ==0){
                array[index] = (byte)0xff;
                index--;
            }else{
                array[index]--;
                return;
            }
        }
        throw new AssertionError("Unable to decrement "+ Arrays.toString(array)+", as it would violate sort-order");
    }



}
