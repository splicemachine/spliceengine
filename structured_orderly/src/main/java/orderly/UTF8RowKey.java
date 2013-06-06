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

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** Serialize and deserialize UTF-8 byte arrays into a sortable byte array
 * representation.
 *
 * <p>This format sorts strings in lexicographic order. The sort order ensures 
 * that NULL &lt; empty string &lt; Character.MIN_CODE_POINT &lt; aa &lt; aaa 
 * &lt; b &lt; ba &lt; bb &lt; ... &lt; Character.MAX_CODE_POINT &lt; ...</p>
 *
 * <h1>Serialization Format</h1>
 * <p>The UTF-8 format already gurantees that sorting byte arrays of UTF-8 
 * encoded characters is equivalent to sorting the equivalent decoded Unicode 
 * strings by Unicode code point. This is discussed further in the 
 * <a href="http://en.wikipedia.org/wiki/UTF-8"> UTF-8 Wikipedia article</a>.
 * As a historical aside, this nifty and very useful property of UTF-8 is due to
 * Ken Thompson and Rob Pike of Unix fame. UTF-8 has many other awesome 
 * properties, like being fully self-synchronized.</p>
 *
 * <p>However, we also need to encode NULL and an end of string terminator.
 * When sorting, we must ensure that NULL &lt; terminator &lt; any valid
 * UTF-8 byte so that strings sort in the correct order. Fortunately, a simple 
 * solution is available to us for encoding NULL and terminator bytes. UTF-8 
 * encoding will never produce the byte values <code>0xff</code> or 
 * <code>0xfe</code>. Thus, we may reserve <code>0x00</code> for NULL and 
 * <code>0x01</code> for terminator if we add 2 to each UTF-8 byte when 
 * serializing the UTF-8 byte array.</p>
 *
 * To encode a NULL, we output 0x0 and return. Otherwise, to encode a non-NULL
 * UTF-8 byte array we add 2 to each of the raw utf-8 bytes and then append the
 * terminator byte at the end. Decoding is simply the reverse of the above 
 * operations.
 *
 * <h1> Descending sort </h1>
 * To sort in descending order we perform the same encodings as in ascending 
 * sort, except we logically invert (take the 1's complement of) each byte, 
 * including the null and termination bytes. 
 *
 * <h1> Implicit Termination </h1>
 * If {@link #termination} is false and the sort order is ascending, we
 * encode NULL values as a zero-length byte array, and omit the terminator byte
 * for every string except the empty string. In this case, our format has zero
 * bytes of overhead versus encoding the raw UTF-8 bytes. The end of the byte 
 * array serves as an implicit terminator byte. Implicit termination is 
 * discussed further in {@link RowKey}.
 *
 * <h1> Usage </h1>
 * This is the fastest class for storing characters and strings. 
 * It performs no object copies during serialization or deserialization.
 */
public class UTF8RowKey extends RowKey 
{
  private static final byte NULL       = (byte)0x00,
                            TERMINATOR = (byte)0x01;

  @Override
  public Class<?> getSerializedClass() { return byte[].class; }

  @Override
  public int getSerializedLength(Object o) throws IOException {
    int term = terminate() ? 1 : 0;
    return o == null ? term : Math.max(((byte[])o).length + term, 1);
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) 
    throws IOException
  {
    byte[] b = w.get();
    int offset = w.getOffset();

    if (o == null) {
      if (terminate()) {
        b[offset] = mask(NULL);
        RowKeyUtils.seek(w, 1);
      }
      return;
    }

    byte[] s = (byte[]) o;
    int len = s.length;

    for (int i = 0; i < len; i++)
      b[offset + i] = mask((byte)(s[i] + 2));

    boolean terminated = terminate() || len == 0;
    if (terminated)
      b[offset + len] = mask(TERMINATOR);
    RowKeyUtils.seek(w, len + (terminated ? 1 : 0));
  }

  protected int getUTF8RowKeyLength(ImmutableBytesWritable w) {
    byte[] b = w.get();
    int offset = w.getOffset(),
        len = w.getLength();

    if (len <= 0)
      return 0;
    if (b[offset] == mask(NULL))
      return 1;

    int i = 0;
    while (i < len && b[offset + i++] != mask(TERMINATOR)) ;
    return i;
  }

  @Override
  public void skip(ImmutableBytesWritable w) throws IOException {
    RowKeyUtils.seek(w, getUTF8RowKeyLength(w));
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    byte[] s = w.get();
    int offset = w.getOffset();
    if (w.getLength() <= 0)
      return null;

    int len = getUTF8RowKeyLength(w);
    try {
      if (s[offset] == mask(NULL))
        return null;
      if (s[offset] == mask(TERMINATOR))
        return RowKeyUtils.EMPTY;

      boolean terminated = s[offset + len - 1] == mask(TERMINATOR);
      byte[] b = new byte[len - (terminated ? 1 : 0)];
      for (int i = 0; i < b.length; i++)
        b[i] = (byte) (mask(s[offset + i]) - 2);
      return b;
    } finally {
      RowKeyUtils.seek(w, len);
    }
  }
}
