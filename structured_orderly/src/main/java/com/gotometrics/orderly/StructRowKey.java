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

import static com.gotometrics.orderly.Termination.SHOULD_NOT;

import java.io.IOException;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

/** Serialize and deserialize a struct (record) row key into a sortable
 * byte array. 
 *
 * <p>A struct row key is a composed of a fixed number of fields.
 * Each field is a subclass of {@link RowKey} (and may even be another struct).
 * The struct is sorted by its field values in the order in which the fields 
 * are declared.</p>
 * 
 * <p>Structs allow for composite row keys. This is similar to a multi-column 
 * primary key or index in MySQL, where the primary key is a struct and the 
 * columns are field row keys.</p>
 *
 * <h1> Serialization Format </h1>
 * The struct row key does not serialize any additional bytes beyond those 
 * used to serialize each of its fields in sequential order. The struct
 * serializes an array of values by simply serializing the first value using
 * the first row key, followed by the second value serialized with the second 
 * row key, and so forth. No bytes are serialized directly by the struct class 
 * at all, so no additional bytes are inserted before, after, or in between 
 * values serialized by the field row keys. In all cases except the implicit
 * termination cases mentioned below, field row keys have {@link #termination}
 * set to true to force each field row key to be self-terminating.
 * 
 * <h1> Implicit and Explicit Termination </h1>
 * The struct row key does not directly serialize any bytes and thus 
 * termination flags do not directly affect the serialization. Instead, 
 * the struct row controls explicit and implicit termination settings by 
 * manipulating the must terminate flags of the field row keys using
 * {@link #setTermination}.
 *
 * <p>If <code>mustTerminate</code> is true, then the mustTerminate flag is set 
 * to true in all field row keys. However, if <code>mustTerminate</code>
 * is false, we can set the must terminate flag to false for any field row key 
 * that is followed by an uninterrupted trailing suffix of ascending-sorted 
 * NULL values. This is because NULL values encoded via ascending sort 
 * are zero-length in all row key formats (assuming <code>mustTerminate</code>
 * is false). In the extreme, a struct with must terminate set to false and all 
 * fields in ascending sort order would serialize a set of null objects to a
 * zero-byte array. All other field row keys (those that are followed by any 
 * serialized value with non-zero length) will have the must terminate flag set
 * to true in the field row key.</p>
 *
 * <h1> Struct Prefixes </h1>
 * Given a serialized struct row key, you may deserialize fields from the 
 * serialized byte representation using any <i>prefix</i> of the struct row 
 * key. For example, if a struct consisting of a long, string, and float
 * is serialized, then fields may be deserialized using a row key consisting of 
 * a long and string, or by using a row key consisting of a single 
 * long. Any trailing fields omitted from the prefix row key will be ignored
 * during deserialization. In the above example, the (long, string) row key 
 * would return a two-element (long object, string object) deserialized result, 
 * ignoring the trailing serialized float as this was not included in the 
 * struct prefix's definition.
 *
 * <h1> NULL </h1>
 * Structs themselves may not be NULL. However, struct fields may be NULL 
 * (so long as the underlying field row key supports NULL), so you can create a 
 * struct where every field is NULL. 
 *
 * <h1> Descending sort </h1>
 * To sort in descending order we invert the sort order of each field row key.
 *
 * <h1> Usage </h1>
 * Structs impose no extra space during serialization, or object copy overhead
 * at runtime. The storage and runtime costs of a struct row key are the
 * sum of the costs of each of its field row keys. 
 *
 * @see StructIterator
 * @see StructBuilder
 */
public class StructRowKey extends RowKey implements Iterable<Object>
{
  private RowKey[] fields;
  private Object[] v;
  private StructIterator iterator;
  private ImmutableBytesWritable iw;

  /** Creates a struct row key object.
   * @param fields - the field row keys of the struct (in declaration order) 
   */
  public StructRowKey(RowKey[] fields) { setFields(fields); }

  @Override
  public RowKey setOrder(Order order) {
    if (order == getOrder())
      return this;

    super.setOrder(order);
    for (RowKey field : fields)
      field.setOrder(field.getOrder() == Order.ASCENDING ? Order.DESCENDING : 
          Order.ASCENDING);
    return this;
  }

  /** Sets the field row keys.
   * @param fields the fields of the struct (in declaration order)
   * @return this object
   */
  public StructRowKey setFields(RowKey[] fields) {
    this.fields = fields; 
    return this;
  }

  /** Gets the field row keys. */
  public RowKey[] getFields() { return fields; }

  @Override
  public Class<?> getSerializedClass() { return Object[].class; }

  private Object[] toValues(Object obj) {
    Object[] o = (Object[]) obj;
    if (o.length != fields.length)
      throw new IndexOutOfBoundsException("Expected " + fields.length 
         + " values but got " + o.length + " values");
    return o;
  }

  /** Initializes mustTerminate in each field row key for the 
   * specified field values. As a side effect of this computation, the 
   * serialized length of the object is computed.
   * @param o field values 
   * @return the serialized length of the field values
   */
  private int setTerminateAndGetLength(Object[] o) throws IOException {
    int len = 0;
    Termination fieldTerm = termination;

    /* We must terminate a field f if (i) mustTerminate is true for this
     * struct or (ii) any field after f has a non-zero deserialized length
     */
    for (int i = o.length - 1; i >= 0; i--) {
      if (fieldTerm == SHOULD_NOT || fields[i].getTermination() != SHOULD_NOT) // SHOULD_NOT always wins
        fields[i].setTermination(fieldTerm);
      int objLen = fields[i].getSerializedLength(o[i]);
      if (objLen > 0) {
        fieldTerm = Termination.MUST;
        len += objLen;
      }
    }

    return len;
  }

  @Override
  public int getSerializedLength(Object obj) throws IOException {
    return setTerminateAndGetLength(toValues(obj));
  }

  @Override
  public void serialize(Object obj, ImmutableBytesWritable w) 
    throws IOException
  {
    Object[] o = toValues(obj);
    setTerminateAndGetLength(o);
    for (int i = 0; i < o.length; i++)
      fields[i].serialize(o[i], w);
  }

  @Override
  public void skip(ImmutableBytesWritable w) throws IOException {
    for (int i = 0; i < fields.length; i++)
      fields[i].skip(w);
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException {
    if (v == null) 
      v = new Object[fields.length];
    for (int i = 0; i < fields.length; i++) 
      v[i] = fields[i].deserialize(w);
    return v;
  }

  /** Sets the serialized row key to iterate over. Subsequent calls to 
   * {@link #iterator} will iterate over this row key.
   * @param iw serialized row key bytes to use for iteration
   * @return this object
   * @see #iterator
   */

  public StructRowKey iterateOver(ImmutableBytesWritable iw) {
    this.iw = iw;
    return this;
  }

  public StructRowKey iterateOver(byte[] b, int offset) {
    if (iw == null)
      iw = new ImmutableBytesWritable();
    iw.set(b, offset, b.length - offset);
    return this;
  }

  public StructRowKey iterateOver(byte[] b) { return iterateOver(b, 0); }

  /** Iterates over a serialized row key. Re-uses the same iterator object
   * across method calls. 
   * @see StructIterator
   * @see #iterateOver
   * @return an iterator for w
   */
  public StructIterator iterator() {
    if (iterator == null)
      iterator = new StructIterator(this);
    iterator.reset();
    iterator.setBytes(iw);
    return iterator;
  }
}
