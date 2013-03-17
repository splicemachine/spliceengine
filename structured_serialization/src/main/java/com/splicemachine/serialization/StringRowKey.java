package com.splicemachine.serialization;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/** Serialize and deserialize Java Strings into row keys.
 * The serialization and deserialization method are identical to 
 * {@link UTF8RowKey} after converting the Java String to/from a UTF-8 byte
 * array.
 *
 * <h1> Usage </h1>
 * This is the slowest class for storing characters and strings. One copy is 
 * made during serialization/deserialization, and furthermore the String
 * objects themselves cannot be re-used across multiple deserializations.
 * Weigh the cost of additional object instantiation
 * and copying against the benefits of being able to use all of the various 
 * handy and tidy String functions in Java.
 */
public class StringRowKey extends UTF8RowKey 
{
  @Override
  public Class<?> getSerializedClass() { return String.class; }

  protected Object toUTF8(Object o) {
    if (o == null || o instanceof byte[])
      return o;
    return Bytes.toBytes((String)o);
  }

  @Override
  public int getSerializedLength(Object o) throws IOException {
    return super.getSerializedLength(toUTF8(o));
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) 
    throws IOException
  {
    super.serialize(toUTF8(o), w);
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    byte[] b = (byte[]) super.deserialize(w);
    return b == null ? b : Bytes.toString(b);
  }
}
