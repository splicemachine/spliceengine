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

import java.util.ArrayList;
import java.util.List;

/** Builds {@link StructRowKey} objects.  */
public class StructBuilder
{
  protected List<RowKey> fields;
  protected Order order;

  public StructBuilder() { 
    this.fields = new ArrayList<RowKey>(); 
    this.order = Order.ASCENDING;
  }

  /** Adds a field row key to the struct definition.
   * @param key field row key to append to the struct definition
   * @return this object
   */
  public StructBuilder add(RowKey key) { fields.add(key); return this; }

  /** Sets a struct field to the specified row key. Fields are numbered 
   * sequentially in the order they are added, starting from 0.
   * @param i struct field definition index
   * @param key row key assigned to field definition
   * @return this object
   */
  public StructBuilder set(int i, RowKey key) {
    fields.set(i, key);
    return this;
  }

  /** Gets the field row key at field index i. */
  public RowKey get(int i) { return fields.get(i); }

  /** Gets all field row keys. */
  public List<RowKey> getFields() { return fields; }

  /** Sets the sort order of the struct. Default is ascending. */
  public StructBuilder setOrder(Order order) {
    this.order = order;
    return this;
  }

  /** Gets the sort order of the struct definition. */
  public Order getOrder() { return order; }

  /** Creates a struct row key. */
  public StructRowKey toRowKey() {
    RowKey[] fields = this.fields.toArray(new RowKey[0]);
    return (StructRowKey) new StructRowKey(fields).setOrder(order);
  }

  /** Resets the struct builder. Removes all fields, sets sort order to 
   * ascending.
   */
  public StructBuilder reset() { 
    fields.clear();
    order = Order.ASCENDING;
    return this;
  }
}
