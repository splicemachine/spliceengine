/*  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing,
 *  software distributed under the License is distributed on an
 *  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 *  KIND, either express or implied.  See the License for the
 *  specific language governing permissions and limitations
 *  under the License.
 */

package orderly;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/** Serializes and deserializes BigDecimal objects into a sortable 
 * byte aray representation.
 *
 * <p>This format ensures that serialized byte values sort in the natural
 * order of a {@link BigDecimal} (as ordered by {@link BigDecimal#compareTo}. 
 * NULL values compare less than any non-NULL value.</p>
 *
 * <h1> Serialization Overview </h1>
 * A <code>BigDecimal</code> object is composed of a power of 10 exponent scale 
 * and an unscaled, arbitrary precision integer significand. The value of the 
 * BigDecimal is unscaled base 2 significand &times; 10<sup>scale</sup>. The 
 * significand is an arbitrary precision {@link BigInteger}, while 
 * the scale is a signed 32-bit int.
 * 
 * <p>This encoding format converts a canonicalized BigDecimal into an a
 * power-of-10 adjustec exponent, and an unscaled arbitrary precision base-10 
 * integer significand. As described in {@link BigDecimal#toString}, an 
 * adjusted exponent is equal to the <code>scale + precision - 1</code>, where 
 * precision is the number of digits in the unscaled base 10 significand 
 * (with trailing zeroes removed).</p>
 * 
 * <p>To serialize the BigDecimal, we first serialize the adjusted exponent 
 * combined with a few leading header bits using a subclass of 
 * {@link IntWritableRowKey} (header bits are packed into the adjusted exponent
 * serialization format using {@link IntWritableRowKey#setReservedValue}). Then
 * we serialize the base 10 significand using a BCD encoding format described 
 * below. The resulting byte array sorts in natural <code>BigDecimal</code> 
 * sort order.</p>
 *
 * <h1> Canonicalization </h1>
 * All BigDecimal values first go through canonicalization by stripping any
 * trailing zeros using the {@link BigDecimal#stripTrailingZeros} method.
 * This avoids having multiple numerically equivalent byte representations, and
 * also ensures that no space is wasted storing redundant trailing zeros.
 *
 * <h1> Base Normalization </h1>
 * Next we convert the arbitrary precision <code>BigInteger</code> significand
 * to base 10, so that the scale and significand have a common base and the
 * adjusted exponent can be calculated. We cannot use two as a common base 
 * because some powers of 10 (such as 10<sup>-1</sup>) have infinite 
 * binary representations. We perform the base 10 conversion on the significand
 * using {@link BigInteger#toString}.  We remove the leading '-' if the 
 * significand value is negative, and encode the resulting decimal String into 
 * bytes using the Binary Coded Decimal format described below. We ignore the
 * significand sign bit when computing the BCD serialization because 
 * the significant sign bit is encoded into the header byte, as described in the
 * Header section.
 *
 * <h1> Zero Nibble Terminated Binary Coded Decimals </h1>
 * We convert decimal Strings into Binary Coded Decimal by mapping 
 * the ASCII characters '0' &hellip; '9' to integers 1 &hellip; 10. Each ASCII 
 * digit is encoded into a 4-bit nibble. There are two nibbles per byte, stored
 * in big-endian order. A nibble of 0 is used as terminator to indicate the end
 * of the BCD encoded string. This ensures that shorter strings that are the 
 * prefix of a longer string will always compare less than the longer string, as
 * the terminator is a smaller value that any decimal digit.
 * 
 * <p>The BCD encoding requires an extra byte of space for the terminator 
 * only if there are an even number of characters (and implicit termination is
 * not allowed). An odd-length string does not use the least significant nibble
 * of its last byte, and thus can store a zero terminator nibble without 
 * requiring any additional bytes.</p>
 *
 * <h1> Exponent </h1>
 * The adjusted exponent is defined as <code>scale + precision - 1</code>, where
 * precision is equal to the number of the digits in the base 10 unscaled 
 * significand. We translate the adjusted exponent into a variable-length byte 
 * array using a subclass of {@link IntWritableRowKey}, with two reserved bits 
 * used to encode header information. 
 * 
 * <p>The adjusted exponent is the sum of two 32-bit integers minus one, which
 * requires 33 bits of storage in the worst case. Given two reserved bits, the 
 * IntWritable row key format can serialize integers with up to 33 data 
 * bits, not including the sign bit. However, this format truncates all values 
 * in memory to fit into a 32-bit integer.</p>
 * 
 * <p>To use the more efficient serialization format employed by 
 * <code>IntWritable</code> while avoiding 32-bit truncation, This class 
 * subclasses <code>IntWritableRowKey</code> to use <code>LongWritable</code> 
 * rather than <code>IntWritable</code> objects for storing values in memory. 
 * The byte serialization format remains unchanged (and is slightly more 
 * efficient for 33-bit objects than the format employed by 
 * {@link LongWritableRowKey}).</p>
 *
 * <h1> Header </h1>
 * The header encodes the type of the BigDecimal: null, negative, zero, or
 * positive. These types are assigned to integer values 0-3, respectively.
 * We use two reserved header bits for the header value, and serialize as part
 * of the adjusted exponent using 
 * <code>IntWritableRowKey.setReservedValue</code>. If the BigDecimal is NULL 
 * or zero, the associated adjusted exponent is also NULL (as there is no finite
 * power of 10 that can produce a value of NULL or zero) and there is no
 * significand. For positive or negative BigDecimals, the adjusted exponent and 
 * significand are always present, and the significand is serialized after the
 * adjusted exponent.
 * 
 * <p>If the header is negative, then all other serialized bits except for the
 * two-bit header are logically inverted. This is to preserve sort order, as 
 * negative numbers with larger exponents or significands should compare less
 * than negative numbers with smaller exponents or significands.</p>
 *
 * <h1> Descending sort </h1>
 * To sort in descending order we perform the same encodings as in ascending 
 * sort, except we logically invert (take the 1's complement of) all 
 * serialized bytes, including the header bits in the adjusted exponent. We 
 * perform this negation on all serialized bytes even if we have already 
 * negated bytes once due to a negative header value.
 *
 * <h1> Implicit Termination </h1>
 * If {@link #termination} is false and the sort order is ascending, we
 * encode NULL values as a zero-length byte array instead of the format
 * specified above. We also omit the trailing terminator byte in our BCD
 * representation (which is only required for even-length BCD serializations
 * anyway). Implicit termination is discussed further in 
 * {@link orderly.RowKey}.
 *
 * <h1> Usage </h1>
 * This is the second fastest class for storing <code>BigDecimal</code> objects.
 * Two copies are performed during serialization and three for deserialization. 
 * Unfortunately, as <code>BigDecimal</code> objects are immutable, they cannot
 * be re-used during deserialization. Each deserialization must allocate a new 
 * <code>BigDecimal</code>. There is currently no available mutable 
 * <code>BigDecimal</code> equivalent. 
 */
public class BigDecimalRowKey extends RowKey 
{
  /* Header types */
  protected static final byte HEADER_NULL = 0x00;
  protected static final byte HEADER_NEGATIVE = 0x01;
  protected static final byte HEADER_ZERO = 0x02;
  protected static final byte HEADER_POSITIVE = 0x03;

  /* Number of header bits */
  protected static final int HEADER_BITS = 0x2;

  protected LongWritable lw;
  protected ExponentRowKey expKey;
  protected byte signMask;

  public BigDecimalRowKey() {
    expKey = new ExponentRowKey();
    expKey.setReservedBits(HEADER_BITS).setTermination(Termination.MUST);
  }

  @Override
  public RowKey setOrder(Order order) {
    expKey.setOrder(order);
    return super.setOrder(order);
  }

  protected byte getSignMask() { return signMask; }

  protected void resetSignMask() { setSignMask((byte)0); }

  protected void setSignMask(byte signMask) { this.signMask = signMask; }

  @Override
  protected byte mask(byte b) {
    return (byte) (b ^ order.mask() ^ signMask);
  }

  @Override
  public Class<?> getSerializedClass() { return BigDecimal.class; }

  /** Gets the length of a String if serialized in our BCD format. We require
   * 1 byte for every 2 characters, rounding up. Furthermore, if the number
   * of characters is even, we require an additional byte for the
   * terminator nibble if terminate() is true.
   */
  protected int getSerializedLength(String s) { 
    return (s.length() + (terminate() ? 2 : 1)) >>> 1; 
  }

  /** Serializes a decimal String s into packed, zero nibble-terminated BCD 
   * format.  After this operation completes, the position (length) of the byte 
   * buffer is incremented (decremented) by the number of bytes written.
   * @param s unsigned decimal string to convert to BCD
   * @param w byte buffer to store the BCD bytes
   */
  protected void serializeBCD(String s, ImmutableBytesWritable w) {
    byte[] b = w.get();
    int offset = w.getOffset(),
        strLength = s.length(),
        bcdLength = getSerializedLength(s);

    for (int i = 0; i < bcdLength; i++) {
      byte bcd = 0; /* initialize both nibbles to zero terminator */
      int strPos = 2 * i;
      if (strPos < strLength)
        bcd = (byte) (1 + Character.digit(s.charAt(strPos), 10) << 4);
      if (++strPos < strLength)
        bcd |= (byte) (1 + Character.digit(s.charAt(strPos), 10));
      b[offset + i] = mask(bcd);
    }

    RowKeyUtils.seek(w, bcdLength);
  }

  /** Converts an arbitrary precision integer to an unsigned decimal string. */
  protected String getDecimalString(BigInteger i) {
    String s = i.toString();
    return i.signum() >= 0 ? s : s.substring(1); /* skip leading '-' */
  }

  @Override
  public int getSerializedLength(Object o) throws IOException {
    if (o == null)
      return terminate() ? expKey.getSerializedLength(null) : 0;

    BigDecimal d = ((BigDecimal)o).stripTrailingZeros();
    BigInteger i = d.unscaledValue();
    if (i.signum() == 0) 
      return expKey.getSerializedLength(null);

    String s = getDecimalString(i);
    if (lw == null)
      lw = new LongWritable();
    lw.set((long)s.length() + -d.scale() -1L);

    return expKey.getSerializedLength(lw) + getSerializedLength(s);
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) 
    throws IOException
  {
    resetSignMask();

    if (o == null) {
      if (terminate()) {
        expKey.setReservedValue(mask(HEADER_NULL));
        expKey.serialize(null, w);
      }
      return;
    }

    BigDecimal d = ((BigDecimal)o).stripTrailingZeros();
    BigInteger i = d.unscaledValue();
    if (i.signum() == 0) {
      expKey.setReservedValue(mask(HEADER_ZERO));
      expKey.serialize(null, w);
      return;
    }

    byte header = i.signum() < 0 ? HEADER_NEGATIVE : HEADER_POSITIVE;
    expKey.setReservedValue(mask(header));

    String s = getDecimalString(i);
    /* Adjusted exponent = precision + scale - 1 */
    long precision = s.length(),
         exp = precision + -d.scale() -1L;
    if (lw == null)
      lw = new LongWritable();
    lw.set(exp);

    setSignMask((byte) (i.signum() >> Integer.SIZE - 1));
    expKey.serialize(lw, w);
    serializeBCD(s, w);
  }

  /** Decodes a Binary Coded Decimal digit and adds it to a string. Returns 
   * true (and leaves string unmodified) if digit is the terminator byte.
   * Returns false otherwise.
   */
  protected boolean addDigit(byte bcd, StringBuilder sb) {
    if (bcd == 0)
      return true;
    sb.append((char) ('0' + bcd - 1));
    return false;
  }

  /** Converts a packed, zero nibble-terminated BCD byte array into an unsigned 
   * decimal String. 
   */
  protected String deserializeBCD(ImmutableBytesWritable w) {
    byte[] b = w.get();
    int offset = w.getOffset(),
        len = w.getLength(),
        i = 0;

    StringBuilder sb = new StringBuilder();
    while(i < len) {
      byte c = mask(b[offset + i++]);
      if (addDigit((byte) ((c >>> 4) & 0xf), sb)
          || addDigit((byte) (c & 0xf), sb))
        break;
    }

    RowKeyUtils.seek(w, i);
    return sb.toString();
  }

  protected int getBCDEncodedLength(ImmutableBytesWritable w) {
    byte[] b = w.get();
    int offset = w.getOffset(),
        len = w.getLength(),
        i = 0;

    while (i < len) {
      byte c = mask(b[offset + i++]);
      if (((c & 0xf0) == 0) || ((c & 0x0f) == 0))
        break;
    }

    return i;
  }

  /** Deserializes BigDecimal header from exponent byte. This method will set
   * sign mask to -1 if header is {@link #HEADER_NEGATIVE}, 0 otherwise.
   * @param b most significant byte of exponent (header byte)
   * @return the BigDecimal header stored in byte b
   */
  protected byte deserializeHeader(byte b) {
    resetSignMask();
    byte h = (byte) ((mask(b) & 0xff) >>> Byte.SIZE - HEADER_BITS);
    setSignMask((byte) (h == HEADER_NEGATIVE ? -1 : 0));
    return h;
  }

  @Override
  public void skip(ImmutableBytesWritable w) throws IOException {
    if (w.getLength() <= 0)
      return;

    byte b = w.get()[w.getOffset()];
    deserializeHeader(b);
    expKey.skip(w);
    if (expKey.isNull(b))
      return;
    RowKeyUtils.seek(w, getBCDEncodedLength(w));
  }
    
  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    byte[] b = w.get();
    int offset = w.getOffset();

    if (w.getLength() <= 0)
      return null;

    byte h = deserializeHeader(b[offset]);
    LongWritable o = (LongWritable) expKey.deserialize(w);
    if (o == null) 
      return h == HEADER_NULL ? null : BigDecimal.ZERO;

    long exp = o.get();
    String s = deserializeBCD(w);

    int precision = s.length(),
        scale = (int) (exp - precision + 1L); 

    BigInteger i = new BigInteger(h == HEADER_POSITIVE ? s : '-' + s);
    return new BigDecimal(i, -scale);
  }

  protected class ExponentRowKey extends IntWritableRowKey {
    /* The maximum value that can be stored by IntWritableRowKey's serialization
     * format (excluding the sign bit) is a 35-bit value, which is enough to 
     * store the 33-bit adjusted exponent + two reserved bits. We override the 
     * Writable methods so that a long is used to store the serialization result
     * in memory, while continuing to use IntWritableRowKey's serialization 
     * format for byte serialization/deserialization.
     */
    @Override
    public Class<?> getSerializedClass() { return LongWritable.class; }

    @Override
    Writable createWritable() { return new LongWritable(); }

    @Override
    void setWritable(long x, Writable w) { ((LongWritable)w).set(x); }

    @Override
    long getWritable(Writable w) { return ((LongWritable)w).get(); }

    @Override
    protected byte mask(byte b) {
      return (byte) (b ^ order.mask() ^ getSignMask());
    }
  }
}
