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
