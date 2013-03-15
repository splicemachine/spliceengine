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

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** Serializes and deserializes Doubles into a sortable byte aray 
 * representation.
 * 
 * <p>The serialization and deserialization method are identical to 
 * {@link DoubleWritableRowKey} after converting the DoubleWritable to/from a 
 * Double.</p>
 *
 * <h1> Usage </h1>
 * This is the slower class for storing doubles. No copies are made when 
 * serializing and deserializing, but unfortunately Double objects are 
 * immutable and thus cannot be re-used across multiple deserializations.
 * However, deserialized primitive doubles are first passed to 
 * {@link Double#valueOf}, so boxed Double values may be shared if the 
 * <code>valueOf</code> method has frequent cache hits.
 */
public class DoubleRowKey extends DoubleWritableRowKey 
{
  private DoubleWritable dw;

  @Override
  public Class<?> getSerializedClass() { return Double.class; }

  protected Object toDoubleWritable(Object o) {
    if (o == null || o instanceof DoubleWritable)
      return o;
    if (dw == null)
      dw = new DoubleWritable();
    dw.set((Double)o);
    return dw;
  }

  @Override
  public int getSerializedLength(Object o) throws IOException {
    return super.getSerializedLength(toDoubleWritable(o));
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) throws IOException {
    super.serialize(toDoubleWritable(o), w);
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    DoubleWritable dw = (DoubleWritable) super.deserialize(w);
    if (dw == null)
      return dw;

    return Double.valueOf(dw.get());
  }
}
