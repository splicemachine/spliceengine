
package com.splicemachine.serialization;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** Serialize and deserialize unsigned integers into fixed-width, sortable 
 * byte arrays. 
 *
 * <p>The serialization and deserialization method are identical to 
 * {@link FixedIntWritableRowKey}, except that the sign bit of the integer is 
 * not negated during serialization.</p>
 *
 * <h1> Usage </h1>
 * This is the fastest class for storing fixed width 32-bit unsigned ints. Use
 * {@link UnsignedIntWritableRowKey} for a more compact, variable-length 
 * representation. This format is more compact only if integers most 
 * frequently require 28 bits or more bits to store.
 */
public class FixedUnsignedIntWritableRowKey extends FixedIntWritableRowKey
{
  protected IntWritable invertSign(IntWritable iw) {
    iw.set(iw.get() ^ Integer.MIN_VALUE);
    return iw;
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) throws IOException {
    invertSign((IntWritable)o);
    super.serialize(o, w);
    invertSign((IntWritable)o);
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    return invertSign((IntWritable) super.deserialize(w));
  }
}
