package com.ir.hbase.hive;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.List;
import org.apache.hadoop.hbase.util.Bytes;

import com.ir.hbase.client.structured.Column;

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
	public static <T> byte[] toBytes(Object value, Column.Type type) {
		if (value == null) return null;
		switch(type) {
	     case BOOLEAN:
				return Bytes.toBytes((Boolean)value);
	     case BYTE:
				return Bytes.toBytes((Byte)value); 
	     case CALENDAR:
				return Bytes.toBytes(((Calendar) value).getTimeInMillis());
	     case DATE:
				return Bytes.toBytes(((Date) value).getTime());
	     case DOUBLE:
				return Bytes.toBytes((Double)value); 
	     case FLOAT:
	    	 	if (value instanceof Double)
	    	 		return Bytes.toBytes((float)((Double) value).doubleValue());
				return Bytes.toBytes((Float)value); 
	     case INTEGER:
				return Bytes.toBytes((Integer)value); 
	     case LONG:
				return Bytes.toBytes((Long)value); 
	     case STRING:
				return Bytes.toBytes((String)value);
		}
	     return null;
	}
	
	@SuppressWarnings("rawtypes")
	public static Object fromBytes(byte[] value, Column.Type type) {
		if (value == null) return null;		
		switch(type) {
		     case BOOLEAN:
		    	 return Bytes.toBoolean(value);
		     case BYTE:
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
		     case CALENDAR:
					Long timeInMillis = Bytes.toLong(value);
					GregorianCalendar gc = new GregorianCalendar();
					gc.setTimeInMillis(timeInMillis);
					return gc;
		     case DATE:
					Long time = Bytes.toLong(value);
					GregorianCalendar g = new GregorianCalendar();
					g.setTimeInMillis(time);
					return g.getTime();
		     case DOUBLE:
			   return Bytes.toDouble(value);
		     case FLOAT:
			   return Bytes.toFloat(value);
		     case INTEGER:
			   return Bytes.toInt(value);
		     case LONG:
			   return Bytes.toLong(value);
		     case STRING:
		       return Bytes.toString(value);
		}
		return null;
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

}
