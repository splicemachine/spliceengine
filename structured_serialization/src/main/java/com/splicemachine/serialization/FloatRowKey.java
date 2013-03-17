
package com.splicemachine.serialization;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** Serializes and deserializes Floats into a sortable byte aray 
 * representation.
 * 
 * <p>The serialization and deserialization method are identical to 
 * {@link FloatWritableRowKey} after converting the FloatWritable to/from a 
 * Float.</p>
 *
 * <h1> Usage </h1>
 * This is the slower class for storing floats. No copies are made when 
 * serializing and deserializing, but unfortunately Float objects are 
 * immutable and thus cannot be re-used across multiple deserializations.
 * However, deserialized primitive floats are first passed to 
 * {@link Float#valueOf}, so boxed Float values may be shared if the 
 * <code>valueOf</code> method has frequent cache hits.
 */
public class FloatRowKey extends FloatWritableRowKey 
{
  private FloatWritable fw;

  @Override
  public Class<?> getSerializedClass() { return Float.class; }

  protected Object toFloatWritable(Object o) {
    if (o == null || o instanceof FloatWritable)
      return o;
    if (fw == null)
      fw = new FloatWritable();
    fw.set((Float)o);
    return fw;
  }

  @Override
  public int getSerializedLength(Object o) throws IOException {
    return super.getSerializedLength(toFloatWritable(o));
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) throws IOException {
    super.serialize(toFloatWritable(o), w);
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    FloatWritable fw = (FloatWritable) super.deserialize(w);
    if (fw == null)
      return fw;

    return Float.valueOf(fw.get());
  }
}
