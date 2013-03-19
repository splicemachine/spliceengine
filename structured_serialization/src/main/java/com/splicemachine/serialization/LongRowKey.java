package com.splicemachine.serialization;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** Serializes and deserializes Long objects into a variable-length
 * sortable byte aray representation.
 *
 * <p>The serialization and deserialization method are identical to 
 * {@link LongWritableRowKey} after converting the LongWritable to/from a
 * Long.</p>
 *
 * <h1> Usage </h1>
 * This is the slower class for storing longs. No copies are made when 
 * serializing and deserializing. Unfortunately Long objects are 
 * immutable and thus cannot be re-used across multiple deserializations.
 * However, deserialized primitive longs are first passed to 
 * {@link Long#valueOf}, so boxed Long values may be shared, reducing the 
 * copies on deserialization, if the <code>valueOf</code> method has frequent 
 * cache hits.
 */
public class LongRowKey extends LongWritableRowKey 
{
  private LongWritable lw;

  @Override
  public Class<?> getSerializedClass() { return Long.class; }

  protected Object toLongWritable(Object o) {
    if (o == null || o instanceof LongWritable)
      return o;
    if (lw == null)
      lw = new LongWritable();
    lw.set((Long)o);
    return lw;
  }

  @Override
  public int getSerializedLength(Object o) throws IOException {
    return super.getSerializedLength(toLongWritable(o));
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) throws IOException {
    super.serialize(toLongWritable(o), w);
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    LongWritable lw = (LongWritable) super.deserialize(w);
    if (lw == null)
      return lw;

    return Long.valueOf(lw.get());
  }
}
