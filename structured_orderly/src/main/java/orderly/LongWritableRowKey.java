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

/** Serialize and deserialize signed, two's complement long integers into a
 * variable-length sortable byte format. 
 *
 * <p>This format ensures that serialized values will sort in their natural 
 * sort order, as determined by (signed) long integer comparison. NULL
 * values compare less than any non-NULL value.</p>
 *
 * <h1>Serialization Format</h1>
 * This variable-length format is a subclass of @{link AbstractVarIntRowKey}.
 * The JavaDoc page for the parent class describes the high-level design of the
 * general serialization format. The basic idea is to only encode only those 
 * bits that have values differing from the (explicit) sign bit. 
 * 
 * <p>Our encoding consists of a header byte followed by 0-8 data bytes. The 
 * data bytes are packed 8-bit data values in big-endian order. The header byte
 * contains the sign bit, the number of data bytes, and the 2-6 most significant
 * bits of data.</p>
 * 
 * <p>The header byte contains both header fields (sign, length) and data. Some
 * header length fields may be omitted in shorter-length encodings, so smaller 
 * encodings contain more data bits in the header. In the case of single-byte 
 * encodings, the header byte contains 6 bits of data. For double-byte 
 * encodings, the header byte contains contains and 5 bits of data. All other 
 * encoding lengths contain 2 bits of data.</p>
 *
 * <p>Thus we encode all numbers in two's complement using the sign bit in the 
 * header and 2<sup>H+D</sup> data bits, where H is the number of data bits in 
 * the header byte and D is the number of data bits in the data bytes 
 * (D = number of data bytes &times; 8). </p>
 * 
 * <p>More specifically, the numerical ranges for our variable-length byte 
 * encoding are:
 * <ul>
 *   <li> One byte: -64 &le; x &le; 63
 *   <li> Two bytes: -8192 &le; x &le; 8191
 *   <li> N &gt; 2 bytes: -2<sup>8 &times; (N-1) + 2</sup> &le; x 
 *        &le; 2<sup>8 &times; (N-1) + 2</sup> - 1
 * </ul>
 * We support all values that can be represented in a java Long, so N &le; 9.
 * </p>
 *
 * <h2> Reserved Bits </h2>
 * Up to two of the most significant bits in the header may be reserved for use
 * by the application, as two is the minimum number of data bits in the header
 * byte. Reserved bits decrease the amount of data stored in the header byte,
 * For example, a single byte encoding with two reserved bits can only encode 
 * integers in the range -16 &le; x &le; 15.
 *
 * <h2> Full Header Format </h2>
 * Given a long integer, x: 
 * <pre>
 * sign = x &gt;&gt; Long.SIZE - 1
 * negSign = ~sign
 * </pre>
 *
 * The full format of the header byte is 
 *
 * <pre>
 * Bit 7:    negSign
 * Bit 6:    single-byte encoded ^ negSign
 * Bit 5:    double-byte encoded ^ negSign
 * Bits 2-4: len ^ sign (each bit XOR'd with original, unnegated sign bit)
 * </pre>
 *
 * <p>Bits 6 and 7 are used in all encodings. If bit 6 indicates a single byte
 * encodng, then bits 0-5 are all data bits. Otherwise, bit 5 is used to
 * indicate a double byte encoding. If a double byte encoding is used,  then 
 * bits 0-4 are data bits. Otherwise, bits 2-4 specify the length of the 
 * extended length (&gt; 2 byte) encoding. In all cases, bits 0-1 are data bits.
 * </p>
 * 
 * <p>The len field represents the (extended) length of the encoded byte array 
 * minus 3, as all extended length serializations must be at least 3 bytes long.
 * In other words, the encoded len field has a bias of +3, so an encoded
 * field with value 1 represents a length of 4 bytes when decoded.
 * The XOR's with sign and negSign are required to preserve sort ordering when
 * using a big-endian byte array comparator to sort the encoded values.</p>
 * 
 * <p>Any padding is done with the sign bit. The worst case space overhead of 
 * this serialization format versus a standard fixed-length encoding is 1 additional 
 * byte. Note that if reserved bits are present, the above header layout is
 * shifted right by the number of reserved bits.</p>
 *
 * <h1> Usage </h1>
 * This is the fastest class for storing signed long integers. It performs no
 * copies during serialization and deserialization, 
 */
public class LongWritableRowKey extends AbstractVarIntRowKey
{
  /** Header flags */
  protected static final byte LONG_SIGN   = (byte) 0x80;
  protected static final byte LONG_SINGLE = (byte) 0x40;
  protected static final byte LONG_DOUBLE = (byte) 0x20;

  /** Header data bits for each header type */
  protected static final int LONG_SINGLE_DATA_BITS = 0x6;
  protected static final int LONG_DOUBLE_DATA_BITS = 0x5;
  protected static final int LONG_EXT_DATA_BITS    = 0x2;

  /** Extended (3-9) byte length attributes */
  /** Number of bits in the length field */
  protected static final int LONG_EXT_LENGTH_BITS = 0x3; 

  public LongWritableRowKey() {
    super(LONG_SINGLE, LONG_SINGLE_DATA_BITS, LONG_DOUBLE,
        LONG_DOUBLE_DATA_BITS, LONG_EXT_LENGTH_BITS, 
        LONG_EXT_DATA_BITS);
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
  long getSign(long l) { return l & Long.MIN_VALUE; }

  @Override
  protected byte initHeader(boolean sign) {
    return sign ? 0 : LONG_SIGN; /* sign bit is negated in header */
  }

  @Override
  protected byte getSign(byte h) { 
    return (h & LONG_SIGN) != 0 ? 0 : Byte.MIN_VALUE;
  }
}
