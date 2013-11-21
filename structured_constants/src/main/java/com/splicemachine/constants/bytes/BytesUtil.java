package com.splicemachine.constants.bytes;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import com.carrotsearch.hppc.BitSet;

/**
 * 
 * This class encapsulates byte[] manipulation with the IR applications.  It relies heavily on HBase's Bytes class.
 * 
 * @author John Leach
 * @version %I%, %G%
 *
 * @see org.apache.hadoop.hbase.util.Bytes
 *
 */
public class BytesUtil {

    /**
     * Concats a list of byte[].
     *
     * @param list
     * @return the result byte array
     */

    public static byte[] concat(List<byte[]> list) {
        int length = 0;
        for (byte[] bytes : list) {
            length += bytes.length;
        }
        byte[] result = new byte[length];
        int pos = 0;
        for (byte[] bytes : list) {
            System.arraycopy(bytes, 0, result, pos, bytes.length);
            pos += bytes.length;
        }
        return result;
    }

    /**
	 * 
	 * Increments a byte[]
	 * 
	 * @param array
	 * @param index
	 */
//	public static void incrementAtIndex(byte[] array, int index) {
//          if (array[index] == Byte.MAX_VALUE) {
//              array[index] = 0;
//              if(index > 0)
//                  incrementAtIndex(array, index - 1);
//          }
//          else {
//              array[index]++;
//          }
//      }

    public static void unsignedIncrement(byte[] array,int index){
        if(array.length<=0) return; //nothing to do
        if(index<0){
            /*
             *  looks like the array is something like [0xFF,0xFF,0xFF,...].
             *
             *  In normal circumstances, we could increment this via rolling bytes over--
             *  e.g. the array becomes [1,0,0,0,...] which has 1 more byte to the left
             *  than the input array.
             *
             *  However, our comparators will sort going from left to right, which means that
             *  rolling over like that will actually place the increment BEFORE the array, instead
             *  of after it like it should. As this violates sort-order restrictions, we are forced
             *  to explode here
             */
            throw new AssertionError("Unable to increment byte[] "+ Arrays.toString(array) +", incrementing would violate sort order");
        }
        int value = array[index] & 0xff;
        if(value==255){
            array[index]=0;
            //we've looped past the entry, so increment the next byte in the array
            unsignedIncrement(array, index-1);
        }else {
            array[index]++;
        }
    }
	
//	public static void decrementAtIndex(byte[] array,int index) {
//		if(array[index] == Byte.MIN_VALUE){
//			array[index] = Byte.MAX_VALUE;
//			if(index >0)
//				decrementAtIndex(array,index-1);
//		}else{
//			array[index]--;
//		}
//	}

    public static void unsignedDecrement(byte[] array, int index){
        if(index<0){
            throw new AssertionError("Unable to decrement "+ Arrays.toString(array)+", as it would violate sort-order");
        }
        if(array[index]==0){
            array[index] = (byte)0xff;
            unsignedDecrement(array,index-1);
        }else
            array[index]--;
    }

//    public static byte[] copyAndIncrement(byte[] start) {
//        if(start.length==0) return new byte[]{1};
//
//        byte[] other = new byte[start.length];
//        System.arraycopy(start,0,other,0,start.length);
//        incrementAtIndex(other,other.length-1);
//        return other;
//    }

    public static String debug(Object o) {
        byte[] bytes = (byte[]) o;
        String s = "" + bytes.length + "[";
        for (int i=0; i<bytes.length; i++) {
            s += " " + bytes[i];
        }
        s += " ]";
        return s;
    }

    public static byte[] concatenate(byte headerByte,byte[] ... bytes){
        int length = 2;
        for(byte[] bytes1:bytes){
            length+=bytes1.length;
        }

        byte[] concatenatedBytes = new byte[length+bytes.length-1];
        concatenatedBytes[0] = headerByte;
        concatenatedBytes[1] = 0x00;
        copyInto(bytes,concatenatedBytes,2);
        return concatenatedBytes;
    }

    public static byte[] concatenate(byte[] ... bytes){
        int length = 0;
        for(byte[] bytes1:bytes){
            length+=bytes1.length;
        }

        byte[] concatenatedBytes = new byte[length+bytes.length-1];
        copyInto(bytes,concatenatedBytes);
        return concatenatedBytes;
    }

    public static byte[] concatenate(byte[][] bytes,int size){
        byte[] concatedBytes;
        if(bytes.length>1)
            concatedBytes = new byte[size+bytes.length-1];
        else
            concatedBytes = new byte[size];
        copyInto(bytes, concatedBytes);
        return concatedBytes;
    }

    private static void copyInto(byte[][] bytes, byte[] concatedBytes,int initialPos){
        int offset = initialPos;
        boolean isStart=true;
        for(byte[] nextBytes:bytes){
            if(nextBytes==null) break;
            if(!isStart){
                concatedBytes[offset] = 0x00; //safe because we know that it's never used in our encoding
                offset++;
            }else
                isStart = false;

            System.arraycopy(nextBytes, 0, concatedBytes, offset, nextBytes.length);
            offset+=nextBytes.length;
        }
    }

    private static void copyInto(byte[][] bytes, byte[] concatedBytes) {
        copyInto(bytes,concatedBytes,0);
    }

    /**
     * Lexicographical byte[] comparator that places empty byte[] values before non-empty values.
     */
    public static Comparator<byte[]> startComparator = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] o1, byte[] o2) {
            return compareBytes(false, o1, o2);
        }
    };

    /**
     * Lexicographical byte[] comparator that places empty byte[] values after non-empty values.
     */
    public static Comparator<byte[]> endComparator = new Comparator<byte[]>() {
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
    private static int compareBytes(boolean emptyGreater, byte[] x, byte[] y) {
        if (empty(x)) {
            if (empty(y)) {
                return 0;
            } else {
                return emptyGreater ? 1 : -1;
            }
        } else if (empty(y)) {
            return emptyGreater ? -1 : 1;
        } else {
            return Bytes.compareTo(x, y);
        }
    }

    /**
     * @return whether or not the given byte[] is empty
     */
    private static boolean empty(byte[] x) {
        return Bytes.compareTo(x, HConstants.EMPTY_BYTE_ARRAY) == 0;
    }

    /**
     * @return a [start, end) pair identifying the ranges of values that are in both [start1, end1) and [start2, end2)
     *         under lexicographic sorting of values.
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
    private static boolean overlap(byte[] start1, byte[] end1, byte[] start2, byte[] end2) {
        return startLessThanEnd(start1, end2) && startLessThanEnd(start2, end1);
    }

    /**
     * @return whether the given start range value is less than the end range value considering lexicographical ordering
     *         and treating empty byte[] as -infinity in starting positions and infinity in ending positions
     */
    private static boolean startLessThanEnd(byte[] start1, byte[] end2) {
        return (empty(end2) || empty(start1) || Bytes.compareTo(start1, end2) < 0);
    }

    /**
     * @return the value that sorts lowest.
     */
    private static byte[] min(Comparator<byte[]> comparator, byte[] x, byte[] y) {
        return (comparator.compare(y, x) <= 0) ? y : x;
    }

    /**
     * @return the value that sorts highest.
     */
    private static byte[] max(Comparator<byte[]> comparator, byte[] x, byte[] y) {
        return (comparator.compare(x, y) <= 0) ? y : x;
    }

    private static final char[] hexArray = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
    public static String toHex(byte[] bytes) {
        if(bytes==null || bytes.length<=0) return "";
        char[] hexChars = new char[bytes.length * 2];
        int v;
        for ( int j = 0; j < bytes.length; j++ ) {
            v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static String toHex(ByteBuffer bytes) {
        if(bytes==null || bytes.remaining()<=0) return "";
        byte[] bits = new byte[bytes.remaining()];
        bytes.get(bits);

        return toHex(bits);
    }

    public static void main(String... args) throws Exception{
        byte[][] vals = new byte[3][];
        vals[0] = new byte[]{0,1};
        vals[1] = new byte[]{2,3};
        vals[2] = new byte[]{4,5};

        System.out.println(Arrays.toString(concatenate(vals,6)));
    }

    public static byte[] unsignedCopyAndIncrement(byte[] start) {
        byte[] next = new byte[start.length];
        System.arraycopy(start,0,next,0,next.length);
        unsignedIncrement(next,next.length-1);
        return next;
    }

    public static void intToBytes(int value,byte[] data,int offset) {
        data[offset] = (byte)(value >>> 24);
        data[offset+1] = (byte)(value >>> 16);
        data[offset+2] = (byte)(value >>> 8);
        data[offset+3] = (byte)(value);
    }

    public static int bytesToInt(byte[] data, int offset) {
        int value = 0;
        value |= (data[offset] & 0xff)<<24;
        value |= (data[offset+1] & 0xff)<<16;
        value |= (data[offset+2] & 0xff)<< 8;
        value |= (data[offset+3] & 0xff);
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
}
