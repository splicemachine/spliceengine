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

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Writable;

/** Serialize and deserialize unsigned long integers into a variable-length 
 * sortable byte format. 
 *
 * <p>This format ensures that serialized values will sort in their natural 
 * sort order, as determined by (unsigned) long integer comparison. NULL
 * values compare less than any non-NULL value. Although we are serializing
 * and deserializing values with a java long, we treat the most significant bit
 * of the long as a data bit, not a sign bit, because this is an unsigned data
 * serialization format.</p>
 *
 * <h1>Serialization Format</h1>
 * This variable-length format is a subclass of {@link AbstractVarIntRowKey}.
 * The JavaDoc page for the parent class describes the high-level design of the
 * general serialization format. The basic idea is to only encode only those 
 * bits that have values differing from the implicit zero-valued sign bit
 * (all unsigned integers effectictively have an implied sign bit of zero).
 * 
 * <p>Our encoding consists of a header byte followed by 0-8 data bytes. The 
 * data bytes are packed 8-bit data values in big-endian order. The header byte
 * contains the number of serialized data bytes, and the 3-7 most significant
 * bits of data.</p>
 * 
 * <p>The header byte contains both header fields (byte length) and data. Some
 * header length fields may be omitted in shorter-length encodings, so smaller 
 * encodings contain more data bits in the header. In the case of single-byte 
 * encodings, the header byte contains 7 bits of data. For double-byte 
 * encodings, the header byte contains contains and 6 bits of data. All other 
 * encoding lengths contain 3 bits of data.</p>
 *
 * <p>Thus we encode all numbers using the 2<sup>H+D</sup> data bits,
 * where H is the number of data bits in the header byte and D is the number of
 * data bits in the data bytes (D = number of data bytes &times; 8).</p>
 * 
 * <p>More specifically, the numerical ranges for our variable-length byte 
 * encoding are:
 * <ul>
 *   <li> One byte: -128 &le; x &le; 127
 *   <li> Two bytes: -16384 &le; x &le; 16383
 *   <li> N &gt; 2 bytes: -2<sup>8 &times; (N-1) + 3</sup> &le; x 
 *        &le; 2<sup>8 &times; (N-1) + 3</sup> - 1
 * </ul>
 * We support all values that can be represented in a java Long, so N &le; 9.
 * </p>
 *
 * <h2> Reserved Bits </h2>
 * Up to three of the most significant bits in the header may be reserved for 
 * use by the application, as three is the minimum number of data bits in the 
 * header byte. Reserved bits decrease the amount of data stored in the header 
 * byte. For example, a single byte encoding with two reserved bits can only 
 * encode integers in the range -32 &le; x &le; 31.
 *
 * <h2> Full Header Format </h2>
 * The full format of the header byte is (note: ~ represents logical negation)
 * <pre>
 * Bit 7:    ~single-byte encoded 
 * Bit 6:    ~double-byte encoded 
 * Bits 3-5: len 
 * </pre>
 *
 * <p>Bit 7 is used in all encodings. If bit 7 indicates a single byte
 * encodng, then bits 0-6 are all data bits. Otherwise, bit 6 is used to
 * indicate a double byte encoding. If a double byte encoding is used, then 
 * bits 0-5 are data bits. Otherwise, bits 3-5 specify the length of the 
 * extended length (&gt; 2 byte) encoding. In all cases, bits 0-2 are data bits.
 * </p>
 * 
 * <p>The len field represents the (extended) length of the encoded byte array 
 * minus 3, as all extended length serializations must be at least 3 bytes long.
 * In other words, the encoded len field has a bias of +3, so an encoded
 * field with value 1 represents a length of 4 bytes when decoded.</p>
 * 
 * <p>Any padding is done with a clear (zero) bit. The worst case space overhead
 * of this serialization format versus a standard fixed-length encoding is 1 
 * additional byte. Note that if reserved bits are present, the above header 
 * layout is shifted right by the number of reserved bits.</p>
 *
 * <h1> Usage </h1>
 * This is the fastest class for storing unsigned long integers. It performs no
 * copies during serialization and deserialization, 
 */
public class UnsignedLongWritableRowKey extends AbstractVarIntRowKey
{
  /** Header flags */
  protected static final byte ULONG_SINGLE = (byte) 0x80;
  protected static final byte ULONG_DOUBLE = (byte) 0x40;

  /** Header data bits for each header type */
  protected static final int ULONG_SINGLE_DATA_BITS = 0x7;
  protected static final int ULONG_DOUBLE_DATA_BITS = 0x6;
  protected static final int ULONG_EXT_DATA_BITS    = 0x3;

  /** Extended (3-9) byte length attributes */
  /** Number of bits in the length field */
  protected static final int ULONG_EXT_LENGTH_BITS = 0x3; 

  public UnsignedLongWritableRowKey() {
    super(ULONG_SINGLE, ULONG_SINGLE_DATA_BITS, ULONG_DOUBLE,
        ULONG_DOUBLE_DATA_BITS, ULONG_EXT_LENGTH_BITS, 
        ULONG_EXT_DATA_BITS);
  }

  @Override
  public Class<?> getSerializedClass() { return LongWritable.class; }

  @Override
  Writable createWritable() { return new LongWritable(); }

  @Override
  void setWritable(long x, Writable w) { ((LongWritable)w).set(x); }

  @Override
  long getWritable(Writable w) { return ((LongWritable)w).get(); }

  @Override
  long getSign(long l) { return 0; }

  @Override
  protected byte initHeader(boolean sign) { return 0; }

  @Override
  protected byte getSign(byte h) { return 0; }

  @Override
  protected byte serializeNonNullHeader(byte b) { return (byte) (b + 1); }

  @Override
  protected byte deserializeNonNullHeader(byte b) { return (byte) (b - 1); }
}
