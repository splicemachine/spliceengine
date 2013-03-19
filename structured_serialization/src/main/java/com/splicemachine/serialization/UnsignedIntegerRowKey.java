package com.splicemachine.serialization;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** Serializes and deserializes Unsigned Integer objects into a variable-length
 * sortable byte aray representation.
 * 
 * <p>The serialization and deserialization methods are identical to 
 * {@link UnsignedIntWritableRowKey} after converting the IntWritable to/from 
 * an Integer</p>.
 *
 * <h1> Usage </h1>
 * This is the slower class for storing unsigned ints. No copies are made 
 * when serializing and deserializing, but unfortunately Integer objects are 
 * immutable and thus cannot be re-used across multiple deserializations.
 * However, deserialized primitive ints are first passed to 
 * {@link Integer#valueOf}, so boxed Integer values may be shared if the 
 * <code>valueOf</code> method has frequent cache hits.
 */
public class UnsignedIntegerRowKey extends UnsignedIntWritableRowKey 
{
  private IntWritable iw;

  @Override
  public Class<?> getSerializedClass() { return Integer.class; }

  protected Object toIntWritable(Object o) {
    if (o == null || o instanceof IntWritable)
      return o;
    if (iw == null)
      iw = new IntWritable();
    iw.set((Integer)o);
    return iw;
  }

  @Override
  public int getSerializedLength(Object o) throws IOException {
    return super.getSerializedLength(toIntWritable(o));
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) throws IOException {
    super.serialize(toIntWritable(o), w);
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    IntWritable iw = (IntWritable) super.deserialize(w);
    if (iw == null)
      return iw;

    return Integer.valueOf(iw.get());
  }
}
