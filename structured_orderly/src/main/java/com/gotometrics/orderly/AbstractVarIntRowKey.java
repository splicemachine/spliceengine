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

package com.gotometrics.orderly;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

/** Serializes and deserializes various integer types a sortable, 
 * variable-length  byte array.
 *
 * <p>Integers, signed or unsigned, are sorted in their natural order. 
 * The serialization format is designed to succinctly represent small absolute 
 * values (i.e. -2 or 4), as these values are the most freqently encountered.
 * Our design is similar in goals to Zig-Zag variable-length integer encoding,
 * but we also ensure that the serialized bytes sort in natural integer sort
 * order.</p>
 *
 * <h1>Serialization Format</h1>
 * Variable-length integers omit all leading bits that are equal to the 
 * sign bit. This means we have a compact, single-byte representation for
 * values like -1 and +1, but require more bytes to serialize values such as 
 * +2^30 and -2^30. 
 * 
 * <p>This abstract class performs serializations to/from a 64-bit long. The
 * encoding uses a header byte followed by 0-8 data bytes. Each data byte is
 * a byte from the serialized integer (in big endian order). The header byte 
 * format consists of implicit or explicit sign bit, a type field (or fields)
 * ndicating the length of the serialized integer in bytes, and the most 
 * significant bits of the serialized integer data.</p>
 * 
 * <p>Operations for setting/getting the header sign bit and type fields, as 
 * well as manipulating Writable objects, are deferred to subclasses. This 
 * design allows subclasses to choose different integer widths as well as 
 * different signedness properties. For example, unsigned integers may be 
 * implemented by treating all integers as having an implicit sign bit set to 
 * zero. Each subclass has a JavaDoc with the full description of the header
 * format used by that particular subclass.</p>
 * 
 * <h1>Reserved Bits</h1>
 * Clients may reserve the most significant bits in the header byte for their 
 * own use. If there are R reservedBits, then the most significant R bits of the 
 * header byte are reserved exclusively for the client and will be initialized 
 * to the client-specified reserved value (default 0) during serialization. The
 * remaining 8-R bits store the header information. Subclasses specify the 
 * maximum number of reserved bits allowed, and typical maximums are 2-3 bits.
 * 
 * <p>Reserved bits are often used to efficently embed variable-length integers 
 * within more complex serialized data structures while preserving sort 
 * ordering. For example, the {@link BigDecimalRowKey} class uses two reserved
 * bits to efficiently embed a variable-length integer exponent within a 
 * serialized BigDecimal object.</p>
 *
 * <h1>NULL</h1>
 * The header byte value 0x00 is reserved for NULL. Subclasses ensure that this
 * value is used for the header byte if and only if the serialized value is
 * NULL.
 *
 * <h1>Implicit Termination</h1>
 * If {@link #termination} is false and the sort order is ascending, we can
 * encode NULL values as a zero-length byte array. Otherwise, the header byte
 * value <code>0x00</code> is used to serialize NULLs. Subclasses ensure this 
 * header byte is used if and only if the serialized value is NULL.
 *
 * <h1>Descending sort</h1>
 * To sort in descending order we perform the same encodings as in ascending 
 * sort, except we logically invert (take the 1's complement of) each byte. 
 * However, any reserved bits in the header byte will not be inverted.
 *
 * <h1> Usage </h1>
 * This is the fastest class for storing integers of any width. It performs no
 * copies during serialization and deserialization, 
 */
public abstract class AbstractVarIntRowKey extends RowKey 
{
  protected static final byte NULL = (byte) 0x00;

  /* An extended length integer has a length >= 3 bytes. Thus we store
   * the encoded length field with a bias of 3 so that we can pack the field 
   * into the minimum number of bits.
   */
  protected static final int HEADER_EXT_LENGTH_BIAS = 0x3;

  /* Header type fields - set by subclass */
  private final byte HEADER_SINGLE;
  private final byte HEADER_DOUBLE;

  /* Number of data bits in header for each header type - set by subclass */
  private final int HEADER_SINGLE_DATA_BITS;
  private final int HEADER_DOUBLE_DATA_BITS;
  private final int HEADER_EXT_DATA_BITS;

  /* Number of bits in the extended header length field  - set by subclass */
  private final int HEADER_EXT_LENGTH_BITS;

  protected Writable lw;
  protected int reservedBits, reservedValue;

  protected AbstractVarIntRowKey(byte headerSingle, int headerSingleDataBits, 
      byte headerDouble, int headerDoubleDataBits, int headerExtLengthBits,
      int headerExtDataBits) 
  {
    this.HEADER_SINGLE = headerSingle;
    this.HEADER_SINGLE_DATA_BITS = headerSingleDataBits;
    this.HEADER_DOUBLE = headerDouble;
    this.HEADER_DOUBLE_DATA_BITS = headerDoubleDataBits;
    this.HEADER_EXT_LENGTH_BITS = headerExtLengthBits;
    this.HEADER_EXT_DATA_BITS = headerExtDataBits;
  }

  /** Creates a writable object for serializing long integers. */
  abstract Writable createWritable();

  /** Stores long integer x to Writable w. */
  abstract void setWritable(long x, Writable w);

  /** Loads a long integer from Writable w. */
  abstract long getWritable(Writable w);

  /** Gets the number of reserved bits in the header byte. */
  public int getReservedBits() { return reservedBits; }

  /** Gets the maximum number of reserved bits.
   * This is equal to the minimum number of data bits in the header,
   * which is always the number of data bits in the extended length 
   * header type. Typical values are 2-3 bits.
   */
  public int getMaxReservedBits() { return HEADER_EXT_DATA_BITS; }

  /** Sets the number of reserved bits in the header byte. Must not exceed
   * the value returned by @{link getMaxReservedBits}. 
   * @param  reservedBits number of reserved header bits
   * @throws IndexOutOfBoundsException  if reservedBits &gt; the maximum number 
   *                                    of reserved bits
   * @return this object
   */
  public AbstractVarIntRowKey setReservedBits(int reservedBits) 
  { 
    if (reservedBits > getMaxReservedBits())
      throw new IndexOutOfBoundsException("Requested " + reservedBits + 
          " reserved bits " + "but only " + getMaxReservedBits() + " permitted");
    this.reservedBits = reservedBits;
    return this;
  }

  /** Sets the reserved value used in the header byte. Values are restricted 
   * to the number of bits specified in @{link setReservedBits}. Any value
   * outside of this range will be automatically truncated to the number of 
   * permitted reserved bits. The value itself is stored in the most 
   * significant bits of the header byte during serialization.
   *
   * @param reservedValue value to place in the header byte
   * @return this object
   */
  public AbstractVarIntRowKey setReservedValue(int reservedValue) {
    this.reservedValue = reservedValue & ((1 << reservedBits) - 1);
    return this;
  }

  /** Gets the reserved header value. */
  public int getReservedValue() { return reservedValue; }

  /** Gets the sign bit of a 64-bit integer x. 
   * @return Long integer with sign bit stored in most significant bit,
   * and all other bits clear
   */
  abstract long getSign(long x);

  /** Reads a byte from long x. Any bytes read past the end of the long are 
   * set to the sign bit.
   * @param byteOffset the offset of the byte to read (starting from the least
   *                   significant byte)
   */
  protected byte readByte(long x, int byteOffset)
  {
    if (byteOffset >= Bytes.SIZEOF_LONG)
      return (byte) (getSign(x) >> Integer.SIZE - 1);
    return (byte) (x >> byteOffset * 8);
  }

  /** Writes byte b to long x. Assumes all bits of x are initialized to the sign
   * bit. Any written past the end of the long have no effect.
   * @param b          the byte to write
   * @param x          the long value to write the byte to
   * @param byteOffset the offset of the byte to write to (starting from the
   *                   least significant byte)
   * @return the result of writing byte b to long x 
   */
  protected long writeByte(byte b, long x, int byteOffset)
  {
    if (byteOffset >= Bytes.SIZEOF_LONG)
      return x;

    /* We only encode bytes where a bit differs from the sign bit, so we OR
     * in 1 bits from byte b if x has its sign bit clear, and mask out/clear 
     * the 0 bits from b if x has its sign bit set. The long casts are 
     * necessary for 64-bit shift offsets (see Java Language Spec. 15.19).
     */
    if (getSign(x) != 0) 
      return x & ~(((long)~b & 0xff) << (byteOffset * 8));
    else 
      return x | (((long)b & 0xff) << (byteOffset * 8));
  }

  @Override
  public int getSerializedLength(Object o) throws IOException {
    if (o == null)
      return terminate() ? 1 : 0;

  /* Compute the number of bits we must store in our variable-length integer
   * serialization. This is the bit position + 1 of the most significant bit 
   * that differs from the sign bit, or zero if all bits are equal to the sign.
   * Reference: Hacker's Delight, 5.3 "Relation to the Log Function", bitsize(x)
   */
    long x = getWritable((Writable)o),
         diffBits = x ^ (getSign(x) >> Long.SIZE - 1);
    int numBits = Long.SIZE - Long.numberOfLeadingZeros(diffBits);    

    if (numBits <=  HEADER_SINGLE_DATA_BITS - reservedBits)
      return 1;
    else if (numBits <= HEADER_DOUBLE_DATA_BITS - reservedBits + 8)
      return 2;

    /* Otherwise, x will require an extended (3-9) byte encoding. The number of
     * data bytes can be computed by calculating one plus the number of
     * bits rounded up to the nearest multiple of 8, after subtracting out the 
     * data bits that can be stored in the header. 
     */
    return 1 + ((numBits - HEADER_EXT_DATA_BITS + reservedBits + 7) >>> 3);
  }

  /** Gets the final masked, serialized null value header byte with reserved 
   * bits set. 
   */
  protected byte getNull() { 
    int nullHeader = mask(NULL) & (0xff >>> reservedBits);
    return (byte) (nullHeader | (reservedValue << Byte.SIZE - reservedBits));
  }

  /** Returns true if the header is for a NULL value. Assumes header is in its
   * final serialized form (masked, reserved bits if any are present).
   */
  protected boolean isNull(byte h) { 
    return (mask(h) & (0xff >>> reservedBits)) == NULL;
  }

  /** Returns a header byte initialized with the specified sign bit. No masking
   * or reserved bit shifts should be performed - this operation should execute
   * as if reservedBits = 0 and order is ascending.
   * @param sign true if the header byte stores an integer with its sign bit set
   * @return header byte initialized with the specified sign bit
   */
  protected abstract byte initHeader(boolean sign);

  /** Returns a header byte after performing any necessary final serialization 
   * operations for non-NULL headers. This is intended to prevent non-NULL
   * header bytes from using the header byte reserved for NULL values. The 
   * header argument has already been shifted right by reservedBits to 
   * make room for the reservedValue. No masking has been performed for sort 
   * ordering (and no masking should be performed by this method).
   * @param h header byte
   * @return header byte after applying all final serialization operations
   */
  protected byte serializeNonNullHeader(byte h) { return h; }

  /** Gets the number of data bits in the header byte.  */
  protected int getNumHeaderDataBits(int length) {
    if (length == 1)
      return HEADER_SINGLE_DATA_BITS - reservedBits;
    else if (length == 2)
      return HEADER_DOUBLE_DATA_BITS - reservedBits;
    return HEADER_EXT_DATA_BITS - reservedBits;
  }

  /** Returns the final serialized header byte for a non-NULL variable-length 
   * integer. 
   * @param sign true if the sign bit of the integer is set
   * @param length length of the serialized integer (in bytes)
   * @param data most significant byte of integer data to be serialized
   * @return serialized, masked header byte 
   */
  protected byte toHeader(boolean sign, int length, byte data) {
    int b = initHeader(sign),
        negSign = sign ? 0 : -1;

    if (length == 1) {
      b |= (~negSign & HEADER_SINGLE);
    } else if (length == 2) {
      b |= (negSign & HEADER_SINGLE) | (~negSign & HEADER_DOUBLE);
    } else {
      int encodedLength = (length - HEADER_EXT_LENGTH_BIAS) ^ ~negSign;
      encodedLength &= (1 << HEADER_EXT_LENGTH_BITS) - 1;
      encodedLength <<= HEADER_EXT_DATA_BITS;
      b |= (negSign & (HEADER_SINGLE|HEADER_DOUBLE)) | encodedLength;
    }

    data &= (1 << getNumHeaderDataBits(length)) - 1;
    b = serializeNonNullHeader((byte) ((b >>> reservedBits) | data));
    b = mask((byte)b) & (0xff >>> reservedBits);
    return (byte) (b | (reservedValue << Byte.SIZE - reservedBits));
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) 
    throws IOException
  {
    byte[] b = w.get();
    int offset = w.getOffset();

    if (o == null) {
      if (terminate()) {
        b[offset] = getNull();
        RowKeyUtils.seek(w, 1);
      }
      return;
    }

    long x = getWritable((Writable)o);
    int length = getSerializedLength((Writable)o);
    
    b[offset] = toHeader(getSign(x) != 0, length, readByte(x, length - 1));
    for (int i = 1; i < length; i++)  
      b[offset + i] = mask(readByte(x, length - i - 1));
    RowKeyUtils.seek(w, length);
  }

  /** Gets the sign of a header byte. The returned value will have the 
   * sign stored its most significant bit, and all other bits clear. Assumes the
   * header byte has its mask and reserved bits, if any, removed (equivalent to
   * a header byte serialized with reservedBits = 0 and order ascending).
   */
  protected abstract byte getSign(byte h);

  /** Performs any initial deserialization operations on a non-NULL header byte.
   * This is intended to undo any work done by @{link serializeNonNullHeader}. 
   * The header byte is assumed to have had its mask if, any removed (equivalent
   * to a header byte serialized in ascending order). However, the header byte 
   * does still contains its reserved bits.
   */
  protected byte deserializeNonNullHeader(byte h) { return h; }

  /** Gets the length in bytes of a variable-length integer from its header. 
   * Assumes the header byte has its mask and reserved bits, if any, 
   * removed (equivalent to a header byte serialized with reservedBits = 0
   * and order ascending).
   */
  protected int getVarIntLength(byte h) {
    int negSign = ~getSign(h) >> Integer.SIZE - 1;
    
    if (((h ^ negSign) & HEADER_SINGLE) != 0) {
      return 1;
    } else if (((h ^ negSign) & HEADER_DOUBLE) != 0) {
      return 2;
    } else {
      int length = ((h ^ ~negSign) >>> HEADER_EXT_DATA_BITS);
      length &= (1 << HEADER_EXT_LENGTH_BITS) - 1;
      return length + HEADER_EXT_LENGTH_BIAS;
    }
  }

  @Override
  public void skip(ImmutableBytesWritable w) throws IOException {
    byte[] b = w.get();
    int offset = w.getOffset();
    if (w.getLength() <= 0)
      return;

    if (isNull(b[offset])) {
      RowKeyUtils.seek(w, 1);
    } else {
      byte h = (byte) (deserializeNonNullHeader(mask(b[offset])));
      RowKeyUtils.seek(w, getVarIntLength((byte) (h << reservedBits)));
    }
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    byte[] b = w.get();
    int offset = w.getOffset();
    if (w.getLength() <= 0)
      return null;

    if (isNull(b[offset])) {
      RowKeyUtils.seek(w, 1);
      return null;
    }

    byte h = (byte) (deserializeNonNullHeader(mask(b[offset])) << reservedBits);
    int length = getVarIntLength(h);

    long x = (long)getSign(h) >> Long.SIZE - 1;
    byte d = (byte) (x << getNumHeaderDataBits(length));
    d |= (byte)((h >>> reservedBits) & ((1 << getNumHeaderDataBits(length))-1));

    x = writeByte(d, x, length - 1);
    for (int i = 1; i < length; i++) 
      x = writeByte(mask(b[offset + i]), x, length - i - 1);
    RowKeyUtils.seek(w, length);

    if (lw == null)
      lw = createWritable();
    setWritable(x, lw);
    return lw;
  }
}
