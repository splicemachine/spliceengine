package com.splicemachine.constants.bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Comparator;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

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
	public static void incrementAtIndex(byte[] array, int index) {
          if (array[index] == Byte.MAX_VALUE) {
              array[index] = 0;
              if(index > 0)
                  incrementAtIndex(array, index - 1);
          }
          else {
              array[index]++;
          }
      }
	
	public static void decrementAtIndex(byte[] array,int index) {
		if(array[index] == Byte.MIN_VALUE){
			array[index] = Byte.MAX_VALUE;
			if(index >0)
				decrementAtIndex(array,index-1);
		}else{
			array[index]--;
		}
	}

    public static byte[] copyAndIncrement(byte[] start) {
        if(start.length==0) return new byte[]{1};

        byte[] other = new byte[start.length];
        System.arraycopy(start,0,other,0,start.length);
        incrementAtIndex(other,other.length-1);
        return other;
    }

    public static String debug(Object o) {
        byte[] bytes = (byte[]) o;
        String s = "" + bytes.length + "[";
        for (int i=0; i<bytes.length; i++) {
            s += " " + bytes[i];
        }
        s += " ]";
        return s;
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

    public static byte[] concatenate(ByteBuffer[] fields){
        int size = 0;
        boolean isFirst=true;
        for (ByteBuffer field : fields) {
            if (isFirst) isFirst = false;
            else
                size++;

            if (field != null)
                size += field.remaining();
        }

        byte[] bytes = new byte[size];
        int offset=0;
        isFirst=true;
        for (ByteBuffer field : fields) {
            if (isFirst) isFirst = false;
            else {
                bytes[offset] = 0x00;
                offset++;
            }
            if (field != null) {
                field.get(bytes, offset, field.remaining());
            }
        }
        return bytes;
    }

    private static void copyInto(byte[][] bytes, byte[] concatedBytes) {
        int offset = 0;
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

    public static Comparator<byte[]> emptyBeforeComparator = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] o1, byte[] o2) {
            if(Bytes.compareTo(o1,HConstants.EMPTY_START_ROW)==0){
                if(Bytes.compareTo(o2,HConstants.EMPTY_START_ROW)==0)
                    return 0;
                else return -1;
            }else if(Bytes.compareTo(o2,HConstants.EMPTY_START_ROW)==0)
                return 1;
            else return Bytes.compareTo(o1,o2);
        }
    };

    public static Comparator<byte[]> emptyAfterComparator = new Comparator<byte[]>() {
        @Override
        public int compare(byte[] o1, byte[] o2) {
            if(Bytes.compareTo(o1,HConstants.EMPTY_START_ROW)==0){
                if(Bytes.compareTo(o2,HConstants.EMPTY_START_ROW)==0)
                    return 0;
                else return 1;
            }else if(Bytes.compareTo(o2,HConstants.EMPTY_START_ROW)==0)
                return -1;
            else return Bytes.compareTo(o1,o2);
        }
    };

    public static Pair<byte[],byte[]> intersect(byte[] a1,byte[] a2, byte[] r1,byte[] r2){
        if(Bytes.compareTo(r2,HConstants.EMPTY_END_ROW)!=0 &&
                Bytes.compareTo(a1,HConstants.EMPTY_START_ROW)!=0 &&
                Bytes.compareTo(r2,a1)<0) return null;
        else if(Bytes.compareTo(r1,HConstants.EMPTY_END_ROW)!=0 &&
                Bytes.compareTo(a2,HConstants.EMPTY_START_ROW)!=0 &&
                Bytes.compareTo(r1,a2)>=0) return null;

        if(emptyBeforeComparator.compare(a1,r1)<=0){
            if(emptyAfterComparator.compare(a2,r2)<=0)
                return Pair.newPair(r1,a2);
            else
                return Pair.newPair(r1,r2);
        }else{
            if(emptyAfterComparator.compare(a2,r2)<=0)
                return Pair.newPair(a1,a2);
            else
                return Pair.newPair(a1,r2);
        }
    }

    private static final char[] hexArray = {'0','1','2','3','4','5','6','7','8','9','A','B','C','D','E','F'};
    public static String toHex(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        int v;
        for ( int j = 0; j < bytes.length; j++ ) {
            v = bytes[j] & 0xFF;
            hexChars[j * 2] = hexArray[v >>> 4];
            hexChars[j * 2 + 1] = hexArray[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static void main(String... args) throws Exception{
        byte[][] vals = new byte[3][];
        vals[0] = new byte[]{0,1};
        vals[1] = new byte[]{2,3};
        vals[2] = new byte[]{4,5};

        System.out.println(Arrays.toString(concatenate(vals,6)));
    }
}
