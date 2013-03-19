package com.splicemachine.serialization;

import java.io.IOException;

import java.math.BigDecimal;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** Serializes and deserializes {@link BigDecimal} Objects into a sortable byte
 * array representation.
 * 
 * <p>This class allows <code>BigDecimal</code> 
 * objects to be lazily deserialized, so that objects are allocated only 
 * when required. The serialization method is identical
 * to {@link BigDecimalRowKey}. The deserialization methods returns
 * an <code>ImmutableBytesWritable</code> object containing the raw serialized
 * bytes. A separate method, {@link #getBigDecimal} (identical to 
 * {@link BigDecimalRowKey#deserialize}) is used to fully deserialize
 * this byte array lazily on demand.</p>
 *
 * <h1> Usage </h1>
 * This class is potentially faster than <code>BigDecimalRowKey</code> as 
 * deserialization is performed lazily. If some values do not have to be fully
 * deserialized, then the client will not pay the object allocation and parsing
 * costs for these values. If all values are fully deserialized, then this class
 * provides no benefits.
 *
 * <p>Two copies are made when serializing and three when fully deserializing. 
 * If full deserialization is not required, then no copies are performed.
 * Unfortunately BigDecimal objects are immutable, and cannot be re-used across
 * multiple calls to the <code>getBigDecimal</code> method.</p>
 */
public class LazyBigDecimalRowKey extends BigDecimalRowKey 
{
  private ImmutableBytesWritable rawBytes;

  @Override
  public Class<?> getDeserializedClass() { 
    return ImmutableBytesWritable.class; 
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    if (rawBytes == null)
      rawBytes = new ImmutableBytesWritable();

    rawBytes.set(w.get(), w.getOffset(), w.getLength());
    super.skip(w);
    return rawBytes;
  }

  /** Gets the <code>BigDecimal</code> stored in the current position of the 
   * byte array. After this method is called, the position (length) of the byte
   * array will be incremented (decremented) by the length of the serialized
   * <code>BigDecimal</code>.
   */
  public BigDecimal getBigDecimal(ImmutableBytesWritable w) throws IOException {
    return (BigDecimal)super.deserialize(w);
  }
}
