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
import org.apache.hadoop.io.LongWritable;

/** Serializes and deserializes Long objects into a variable-length
 * sortable byte aray representation.
 *
 * <p>The serialization and deserialization method are identical to 
 * {@link LongWritableRowKey} after converting the LongWritable to/from a
 * Long.</p>
 *
 * <h1> Usage </h1>
 * This is the slower class for storing longs. No copies are made when 
 * serializing and deserializing. Unfortunately Long objects are 
 * immutable and thus cannot be re-used across multiple deserializations.
 * However, deserialized primitive longs are first passed to 
 * {@link Long#valueOf}, so boxed Long values may be shared, reducing the 
 * copies on deserialization, if the <code>valueOf</code> method has frequent 
 * cache hits.
 */
public class LongRowKey extends LongWritableRowKey 
{
  private LongWritable lw;

  @Override
  public Class<?> getSerializedClass() { return Long.class; }

  protected Object toLongWritable(Object o) {
    if (o == null || o instanceof LongWritable)
      return o;
    if (lw == null)
      lw = new LongWritable();
    lw.set((Long)o);
    return lw;
  }

  @Override
  public int getSerializedLength(Object o) throws IOException {
    return super.getSerializedLength(toLongWritable(o));
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) throws IOException {
    super.serialize(toLongWritable(o), w);
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    LongWritable lw = (LongWritable) super.deserialize(w);
    if (lw == null)
      return lw;

    return Long.valueOf(lw.get());
  }
}
