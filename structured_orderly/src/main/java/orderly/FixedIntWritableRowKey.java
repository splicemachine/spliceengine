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
import org.apache.hadoop.io.IntWritable;

/** Serializes and deserializes IntWritablesWritables into a sortable 
 * fixed-length byte array representation.
 *
 * <p>This format ensures that all integers sort in their natural order, as
 * they would sort when using signed integer comparison.</p>
 *
 * <h1>Serialization Format</h1>
 * All Integers are serialized to a 4-byte, fixed-width sortable byte format.
 * Serialization is performed by inverting the integer sign bit and writing the
 * resulting bytes to the byte array in big endian order. 
 *
 * <h1>NULL</h1>
 * Like all fixed-width integer types, this class does <i>NOT</i> support null
 * value types. If you need null support use {@link IntWritableRowKey}.
 *
 * <h1>Descending sort</h1>
 * To sort in descending order we perform the same encodings as in ascending 
 * sort, except we logically invert (take the 1's complement of) each byte. 
 *
 * <h1>Usage</h1>
 * This is the fastest class for storing fixed width 32-bit ints. Use 
 * {@link IntWritableRowKey} for a more compact, variable-length representation
 * in almost all cases. This format is only more compact if integers most
 * frequently require 28 or more bits to store (including the sign bit).
 */
public class FixedIntWritableRowKey extends RowKey 
{
  private IntWritable iw;

  @Override
  public Class<?> getSerializedClass() { return IntWritable.class; }

  @Override
  public int getSerializedLength(Object o) throws IOException {
    return Bytes.SIZEOF_INT;
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) 
    throws IOException
  {
    byte[] b = w.get();
    int offset = w.getOffset();

    int i = ((IntWritable)o).get();
    Bytes.putInt(b, offset, i ^ Integer.MIN_VALUE ^ order.mask());
    RowKeyUtils.seek(w, Bytes.SIZEOF_INT);
  }

  @Override
  public void skip(ImmutableBytesWritable w) throws IOException {
    RowKeyUtils.seek(w, Bytes.SIZEOF_INT);
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    int offset = w.getOffset();
    byte[] s = w.get();

    int i = Bytes.toInt(s, offset) ^ Integer.MIN_VALUE ^ order.mask();
    RowKeyUtils.seek(w, Bytes.SIZEOF_INT);

    if (iw == null)
      iw = new IntWritable();
    iw.set(i);
    return iw;
  }
}
