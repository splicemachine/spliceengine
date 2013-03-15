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

import java.util.Arrays;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** Various utility functions for creating and manipulating row keys. */
public class RowKeyUtils 
{
  /** Shared (immutable) zero-length byte array singleton. */
  public static final byte[] EMPTY = new byte[0];

  /** Converts a (byte array, offset, length) triple into a byte array,
   * copying only if necessary. No copy is performed if offset is 0 and
   * length is array.length. 
   */
  public static byte[] toBytes(byte[] b, int offset, int length) {
    if (offset == 0 && length == b.length) 
      return b;
    else if (offset == 0)
      return Arrays.copyOf(b, length);
    return Arrays.copyOfRange(b, offset, offset + length);
  }

  /** Converts an ImmutableBytesWritable to a byte array, copying only if
   * necessary.
   */
  public static byte[] toBytes(ImmutableBytesWritable w) {
    return toBytes(w.get(), w.getOffset(), w.getLength());
  }

  /** Converts a Text object to a byte array, copying only if
   * necessary.
   */
  public static byte[] toBytes(Text t) {
    return toBytes(t.getBytes(), 0, t.getLength());
  }

  /** Seeks forward/backward within an ImmutableBytesWritable. After
   * seek is complete, the position (length) of the byte array is 
   * incremented (decremented) by the seek amount.
   * @param w  immutable byte array used for seek
   * @param offset number of bytes to seek (relative to current position)
   */
  public static void seek(ImmutableBytesWritable w, int offset) {
    w.set(w.get(), w.getOffset() + offset, w.getLength() - offset);
  }
}
