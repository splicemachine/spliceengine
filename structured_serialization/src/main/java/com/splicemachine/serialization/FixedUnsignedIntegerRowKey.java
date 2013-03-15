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

/** Serialize and deserialize Unsigned Integer Objects into a fixed-length 
 * sortable byte array representation. 
 *
 * <p>The serialization and deserialization method are identical to 
 * {@link FixedUnsignedIntWritableRowKey} after converting the IntWritable 
 * to/from an Integer.</p>
 *
 * <h1> Usage </h1>
 * This is the slower class for storing unsigned ints. Only one copy is made 
 * when serializing and deserializing, but unfortunately Integer objects are 
 * immutable and thus cannot be re-used across multiple deserializations.
 * However, deserialized primitive ints are first passed to 
 * {@link Integer#valueOf}, so boxed Integer values may be shared if the 
 * <code>valueOf</code> method has frequent cache hits.
 */
public class FixedUnsignedIntegerRowKey extends FixedUnsignedIntWritableRowKey 
{
  private IntWritable iw;

  @Override
  public Class<?> getSerializedClass() { return Integer.class; }

  protected Object toIntWritable(Object o) {
    if (o == null || o instanceof IntWritable)
      return o;
    if (iw == null)
      iw = new IntWritable();
    iw.set((Integer)o);
    return iw;
  }

  @Override
  public int getSerializedLength(Object o) throws IOException {
    return super.getSerializedLength(toIntWritable(o));
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) throws IOException {
    super.serialize(toIntWritable(o), w);
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    IntWritable iw = (IntWritable) super.deserialize(w);
    if (iw == null)
      return iw;

    return Integer.valueOf(iw.get());
  }
}
