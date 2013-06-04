package com.splicemachine.constants.bytes;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.*;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
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
	 * 
	 * Method to return bytes based on an object and its corresponding class.
	 * 
	 * @param value
	 * @param instanceClass
	 * @return byte array of the object and its corresponding class
	 */
	public static <T> byte[] toBytes(Object value, Class<T> instanceClass) {
		if (value == null) return null;
		if ((instanceClass == Boolean.class) || (instanceClass == boolean.class))
			return Bytes.toBytes((Boolean)value);
		else if ((instanceClass == Byte.class) || (instanceClass == byte.class))
			return Bytes.toBytes((Byte)value); 
		else if ((instanceClass == Character.class) || (instanceClass == char.class))
			return Bytes.toBytes((Character)value); 
		else if ((instanceClass == Double.class) || (instanceClass == double.class))
			return Bytes.toBytes((Double)value); 
		else if ((instanceClass == Float.class) || (instanceClass == float.class))
			return Bytes.toBytes((Float)value); 
		else if ((instanceClass == Integer.class) || (instanceClass == int.class))			
			return Bytes.toBytes((Integer)value); 
		else if ((instanceClass == Long.class) || (instanceClass == long.class))
			return Bytes.toBytes((Long)value); 
		else if ((instanceClass == Short.class) || (instanceClass == short.class))
			return Bytes.toBytes((Short)value); 
		else if (instanceClass == Date.class)
			return Bytes.toBytes(((Date)value).getTime());
		else if (instanceClass == Calendar.class)
			return Bytes.toBytes(((Calendar)value).getTime().getTime());
		else if (instanceClass == String.class) {
			return Bytes.toBytes((String)value);
		}
		else if (value instanceof Calendar)
			return Bytes.toBytes(((Calendar) value).getTimeInMillis());
		try {
			ByteArrayOutputStream bos = new ByteArrayOutputStream();
			ObjectOutputStream oos = new ObjectOutputStream(bos);
			oos.writeObject(value);

			byte[] Out = bos.toByteArray();
			oos.close();
			bos.close();
			return Out;
		}
		catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
	}
	/**
	 * 
	 * Returns an object from its byte array representation.
	 * 
	 * @param value
	 * @param valueType
	 * @return the object converted from bytes array
	 */
	@SuppressWarnings("rawtypes")
	public static Object fromBytes(byte[] value, Class valueType) {
		if (value == null) return null;

		if ((valueType == Boolean.class) || (valueType == boolean.class))
			return Bytes.toBoolean(value);
		else if ((valueType == Byte.class) || (valueType == byte.class)) {
			byte out;
			try {
				ByteArrayInputStream bis = new ByteArrayInputStream(value);
				ObjectInputStream ois = new ObjectInputStream(bis);
				out = ois.readByte();
				ois.close();
				bis.close();
			}
			catch (IOException e) {
				throw new RuntimeException(e.getMessage(), e);
			}
			return out;
		} 
		else if ((valueType == Character.class) || (valueType == char.class)) {
			char out;
			try {
				ByteArrayInputStream bis = new ByteArrayInputStream(value);
				ObjectInputStream ois = new ObjectInputStream(bis);
				out = ois.readChar();
				ois.close();
				bis.close();
			}
			catch (IOException e) {
				throw new RuntimeException(e.getMessage(), e);
			}
			return out;    
		} 
		else if ((valueType == Double.class) || (valueType == double.class))
			return Bytes.toDouble(value); 
		else if ((valueType == Float.class) || (valueType == float.class))
			return Bytes.toFloat(value); 
		else if ((valueType == Integer.class) || (valueType == int.class))
			return Bytes.toInt(value); 
		else if ((valueType == Long.class) || (valueType == long.class))
			return Bytes.toLong(value); 
		else if ((valueType == Short.class) || (valueType == short.class))
			return Bytes.toShort(value); 
		else if (valueType == String.class)
			return Bytes.toString(value); 
		else if (valueType == Calendar.class || valueType == GregorianCalendar.class) {
			Long timeInMillis = Bytes.toLong(value);
			GregorianCalendar gc = new GregorianCalendar();
			gc.setTimeInMillis(timeInMillis);
			return gc;
		}
		else if (valueType == Date.class) {
			Long timeInMillis = Bytes.toLong(value);
			GregorianCalendar gc = new GregorianCalendar();
			gc.setTimeInMillis(timeInMillis);
			return gc.getTime();
		}

		
		Object out;
		try {
			ByteArrayInputStream bis = new ByteArrayInputStream(value);
			ObjectInputStream ois = new ObjectInputStream(bis);
			out = ois.readObject();
			ois.close();
			bis.close();
		}
		catch (IOException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		catch (ClassNotFoundException e) {
			throw new RuntimeException(e.getMessage(), e);
		}
		return out;
	}
	/**
	 * 
	 * Retrieves the Typed Object from its byte[] representation.
	 * 
	 * @param value
	 * @param valueType
	 * @return the type
	 */
	@SuppressWarnings("unchecked")
	public static <T> T fromBytesToType(byte[] value, Class<T> valueType) {
		return (T) fromBytes(value, valueType);
	}

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

    public static byte[] concatenate(byte[][] bytes,int size){
        byte[] concatedBytes;
        if(bytes.length>1)
            concatedBytes = new byte[size+bytes.length-1];
        else
            concatedBytes = new byte[size];
        int offset = 0;
        boolean isStart=true;
        for(byte[] nextBytes:bytes){
            if(nextBytes==null) break;
            if(!isStart){
                concatedBytes[offset] = 1;
                offset++;
            }else
                isStart = false;

            System.arraycopy(nextBytes, 0, concatedBytes, offset, nextBytes.length);
            offset+=nextBytes.length;
        }
        return concatedBytes;
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
            if(emptyBeforeComparator.compare(a2,r1)<=0)
                return null;
            else if(emptyAfterComparator.compare(a2,r2)<=0)
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

    public static void main(String... args) throws Exception{
        byte[][] vals = new byte[3][];
        vals[0] = new byte[]{0,1};
        vals[1] = new byte[]{2,3};
        vals[2] = new byte[]{4,5};

        System.out.println(Arrays.toString(concatenate(vals,6)));
    }
}
