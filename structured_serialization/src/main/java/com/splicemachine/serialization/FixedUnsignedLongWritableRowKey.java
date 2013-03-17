
package com.splicemachine.serialization;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** Serialize and deserialize unsigned long integers into fixed-width, sortable 
 * byte arrays. 
 *
 * <p>The serialization and deserialization method are identical to 
 * {@link FixedLongWritableRowKey}, except the sign bit of the long is not
 * negated during serialization.</p>
 *
 * <h1> Usage </h1>
 * This is the fastest class for storing fixed width 64-bit unsigned ints. Use 
 * {@link UnsignedLongWritableRowKey} for a more compact, variable-length 
 * representation. This format is more compact only if integers most frequently
 * require 59 or more bits to store.
 */
public class FixedUnsignedLongWritableRowKey extends FixedLongWritableRowKey
{
  protected LongWritable invertSign(LongWritable lw) {
    lw.set(lw.get() ^ Long.MIN_VALUE);
    return lw;
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) throws IOException {
    invertSign((LongWritable)o);
    super.serialize(o, w);
    invertSign((LongWritable)o);
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    return invertSign((LongWritable) super.deserialize(w));
  }
}
