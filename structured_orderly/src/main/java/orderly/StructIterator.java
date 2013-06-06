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
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** Iterates over a serialized {@link StructRowKey}. This iterates over each
 * serialized row key field from the specified struct definition, and for each 
 * field allows you to deserialize the field or skip over its serialized bytes. 
 * In contrast, the methods provided by <code>StructRowKey</code>, 
 * {@link StructRowKey#deserialize} and {@link StructRowKey#skip}, 
 * deserialize or skip the entire struct at once (respectively).
 *
 * <p>A deserialized field has the same type as the field row key's deserialized
 * class (specified by {@link RowKey#getDeserializedClass}). This iterator may 
 * also be used to deserialize bytes from any <code>StructRowKey</code> for 
 * which the specified row key is a prefix. For example, if the specified 
 * struct row key definition has a long and an integer field, you may parse the
 * serialized output of a struct whose fields are a long, an integer, and a 
 * string. The iterator would return a long followed by an integer, and the 
 * trailing string would be ignored.</p>
 */
public class StructIterator implements Iterator<Object>
{
  private StructRowKey rowKey;
  private RowKey[] fields;
  private int fieldPos, origOffset, origLength;
  private ImmutableBytesWritable w;

  /** Creates a struct row key iterator.
   * @param rowKey the struct row key type to use for deserialization
   */
  public StructIterator(StructRowKey rowKey) { setRowKey(rowKey); }

  /** Creates a struct row key iterator.
   * @param rowKey the struct row key type to use for deserialization
   * @param bytes the serialized bytes to read from
   */
  public StructIterator(StructRowKey rowKey, ImmutableBytesWritable bytes) {
    setRowKey(rowKey);
    setBytes(bytes);
  }

  /** Creates a struct row key iterator. */
  public StructIterator() { }

  /** Sets the struct row key used for deserialization. */
  public StructIterator setRowKey(StructRowKey rowKey) {
    this.rowKey = rowKey;
    this.fields = rowKey.getFields();
    return this;
  }

  /** Gets the struct row key used for deserialization. */
  public StructRowKey getRowKey() { return rowKey; }

  /** Sets the serialized byte array to read from. */
  public StructIterator setBytes(ImmutableBytesWritable w) {
    this.w = w;
    this.fieldPos = 0;
    this.origOffset = w.getOffset();
    this.origLength = w.getLength();
    return this;
  }

  /** Gets the serialized byte array to read from. The array offset and length
   * are set to the current position.
   */
  public ImmutableBytesWritable getBytes() { return w; }

  /** Resets the read position to the beginning of the serialized byte array */
  public void reset() { 
    this.fieldPos = 0;
    if (w != null)
      w.set(w.get(), origOffset, origLength);
  }

  /** Skips the current field and increments read position by the number
   * of bytes read. 
   */
  public void skip() throws IOException { fields[fieldPos++].skip(w); }

  /** Deserializes the current field and increments read position by the
   * number of bytes read. 
   */
  public Object deserialize() throws IOException {
    return fields[fieldPos++].deserialize(w);
  }

  public boolean hasNext() { return fieldPos < fields.length; }

  public Object next() { 
    try {
      if (!hasNext())
        throw new NoSuchElementException();
      return deserialize(); 
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void remove() { throw new UnsupportedOperationException(); }
}
