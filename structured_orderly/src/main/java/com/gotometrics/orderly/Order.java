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

/** The sort order of a row key, ascending or descending. */
public enum Order
{ 
  ASCENDING((byte)0), 
  DESCENDING((byte)0xff);

  private final byte mask;

  Order(byte mask) { this.mask = mask; }

  /** Gets the byte mask associated with the sort order. When a 
   * serialized byte is XOR'd with the mask, the result is the same byte 
   * but sorted in the direction specified by the Order object. 
   * @see RowKey#serialize
   */
  byte mask() { return mask; }

}
