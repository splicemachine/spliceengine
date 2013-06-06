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
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.FloatWritable;

/** Serializes and deserializes FloatWritables into a sortable byte aray 
 * representation.
 *
 * <p>This format ensures the following total ordering of floating point values:
 * NULL &lt; Float.NEGATIVE_INFINITY &lt; -Float.MAX_VALUE &lt; ... 
 * &lt; -Float.MIN_VALUE &lt; -0.0 &lt; +0.0; &lt; Float.MIN_VALUE &lt; ...
 * &lt; Float.MAX_VALUE &lt; Float.POSITIVE_INFINITY &lt; Float.NaN</p>
 *
 * <h1>Serialization Format</h1>
 * <p>Floating point numbers are encoded as specified in IEEE 754. A 32-bit 
 * single precision float consists of a sign bit, 8-bit unsigned exponent 
 * encoded in offset-127 notation, and a 23-bit significand. The format is 
 * described further in the <a href="http://en.wikipedia.org/wiki/Single_precision">
 * Single Precision Floating Point Wikipedia page</a></p>
 *
 * <p>The value of a normal float is 
 * -1 <sup>sign bit</sup> &times; 2<sup>exponent - 127</sup>
 * &times; 1.significand</p>
 *
 * <p>The IEE754 floating point format already preserves sort ordering 
 * for positive floating point numbers when the raw bytes are compared in 
 * most significant byte order. This is discussed further at 
 * <a href="http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm">
 * http://www.cygnus-software.com/papers/comparingfloats/comparingfloats.htm</a>
 * </p>
 *
 * <p>Thus, we need only ensure that negative numbers sort in the the exact 
 * opposite order as positive numbers (so that say, negative infinity is less 
 * than negative 1), and that all negative numbers compare less than any 
 * positive number. To accomplish this, we invert the sign bit of all floating 
 * point numbers, and we also invert the exponent and significand bits if the 
 * floating point number was negative.</p>
 *
 * <p>More specifically, we first store the floating point bits into a 32-bit 
 * int <code>j</code> using {@link Float#floatToIntBits}. This method 
 * collapses all NaNs into a single, canonical NaN value but otherwise leaves 
 * the bits unchanged. We then compute</p>
 *
 * <pre>
 * j ^= (j &gt;&gt; (Integer.SIZE - 1)) | Integer.MIN_SIZE
 * </pre>
 *
 * <p>which inverts the sign bit and XOR's all other bits with the sign bit 
 * itself. Comparing the raw bytes of <code>j</code> in most significant byte 
 * order is equivalent to performing a single precision floating point 
 * comparison on the underlying bits (ignoring NaN comparisons, as NaNs don't 
 * compare equal to anything when performing floating point comparisons).</p>
 *
 * <p>Finally, we must encode NULL efficiently. Fortunately, <code>j</code> can 
 * never have all of its bits set to one (equivalent to -1 signed in two's 
 * complement) as this value corresponds to a NaN removed during NaN 
 * canonicalization. Thus, we can encode NULL as zero, and all non-NULL 
 * numbers are translated to an int as specified above and then incremented by 
 * 1, which is guaranteed not to cause unsigned overflow as <code>j</code> must
 * have at least one bit set to zero.</p>
 *
 * <p>The resulting integer is then converted into a byte array by 
 * serializing the integer one byte at a time in most significant byte order.
 * All serialized values are 4 bytes in length</p>
 *
 * <h1> Descending sort </h1>
 * To sort in descending order we perform the same encodings as in ascending 
 * sort, except we logically invert (take the 1's complement of) each byte. 
 *
 * <h1> Implicit Termination </h1>
 * If {@link #termination} is false and the sort order is ascending, we can
 * encode NULL values as a zero-length byte array instead of using the 8 byte 
 * encoding specified above. Implicit termination is discussed further in 
 * {@link RowKey}.
 *
 * <h1> Usage </h1>
 * This is the fastest class for storing floats. It performs no object copies
 * during serialization and deserialization.
 */
public class FloatWritableRowKey extends RowKey 
{
  private static final int NULL = 0;
  private FloatWritable fw;

  @Override
  public Class<?> getSerializedClass() { return FloatWritable.class; }

  @Override
  public int getSerializedLength(Object o) throws IOException {
    if (o == null && !terminate())
      return 0;
    return Bytes.SIZEOF_INT;
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) 
    throws IOException
  {
    byte[] b = w.get();
    int offset = w.getOffset();
    int j;

    if (o == null) {
      if (!terminate())
        return;
      j = NULL;
    } else {
      j = Float.floatToIntBits(((FloatWritable)o).get());
      j = (j ^ ((j >> Integer.SIZE - 1) | Integer.MIN_VALUE)) + 1; 
    }
   
    Bytes.putInt(b, offset, j ^ order.mask());
    RowKeyUtils.seek(w, Bytes.SIZEOF_INT);
  }

  @Override
  public void skip(ImmutableBytesWritable w) throws IOException {
    if (w.getLength() <= 0)
      return;
    RowKeyUtils.seek(w, Bytes.SIZEOF_INT);
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    byte[] s = w.get();
    int offset = w.getOffset();
    if (w.getLength() <= 0)
      return null;

    try {
      int j = Bytes.toInt(s, offset) ^ order.mask();

      if (j == NULL)
        return null;

      if (fw == null)
        fw = new FloatWritable();

      j--;
      j ^= (~j >> Integer.SIZE - 1) | Integer.MIN_VALUE;
      fw.set(Float.intBitsToFloat(j));
      return fw;
    } finally {
      RowKeyUtils.seek(w, Bytes.SIZEOF_INT);
    }
  }
}
