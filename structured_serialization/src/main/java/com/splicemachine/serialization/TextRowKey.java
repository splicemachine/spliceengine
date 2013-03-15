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

import org.apache.hadoop.io.Text;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;

/** Serialize and deserialize Hadoop Text Objects into sortable byte arrays.
 * The serialization and deserialization method are identical to 
 * {@link UTF8RowKey} after converting the Text to/from a UTF-8 byte
 * array.
 *
 * <h1> Usage </h1>
 * This is the second fastest class for storing characters and strings. No
 * objetc copies are made when serializing. Unfortunately, there is no way to
 * force a Text object to use an existing byte array without copying its 
 * contents, so one copy is required when deserializing. This class re-uses
 * Text objects during deserialization, although the byte array
 * backing the Text object is not re-used.
 */
public class TextRowKey extends UTF8RowKey 
{
  private Text t;

  @Override
  public Class<?> getSerializedClass() { return Text.class; }

  protected Object toUTF8(Object o) {
    if (o == null || o instanceof byte[])
      return o;
    return RowKeyUtils.toBytes((Text)o);
  }

  @Override
  public int getSerializedLength(Object o) throws IOException {
    return super.getSerializedLength(toUTF8(o));
  }

  @Override
  public void serialize(Object o, ImmutableBytesWritable w) throws IOException {
    super.serialize(toUTF8(o), w);
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    byte[] b = (byte[]) super.deserialize(w);
    if (b == null)
      return b;

    if (t == null)
      t = new Text();
    t.set(b);
    return t;
  }
}
