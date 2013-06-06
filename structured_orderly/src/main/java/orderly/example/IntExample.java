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

package orderly.example;


import orderly.IntWritableRowKey;
import orderly.IntegerRowKey;
import orderly.Order;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public class IntExample
{
  /* Simple examples showing serialization lengths with Integer Row Key */
  public void lengthExamples() throws Exception {
    IntegerRowKey i = new IntegerRowKey();

    System.out.println("serialize(null) length - " + i.serialize(null).length);
    System.out.println("serialize(57) length - " + i.serialize(57).length);
    System.out.println("serialize(293) length - " + i.serialize(293).length);

    i.setOrder(Order.DESCENDING);
    System.out.println("descending serialize (null) - length " + 
        i.serialize(null).length);
    System.out.println("descending serialize (57) - length " + 
        i.serialize(57).length);
  }

  /* Simple examples showing serialization tests with IntWritable Row Key */
  public void serializationExamples() throws Exception {
    IntWritableRowKey i = new IntWritableRowKey();
    IntWritable w = new IntWritable();
    ImmutableBytesWritable buffer = new ImmutableBytesWritable();
    byte[] b;

    /* Serialize and deserialize into an immutablebyteswritable */
    w.set(-93214);
    b = new byte[i.getSerializedLength(w)];
    buffer.set(b);
    i.serialize(w, buffer);
    buffer.set(b, 0, b.length);
    System.out.println("deserialize(serialize(-93214)) = " + 
        ((IntWritable)i.deserialize(buffer)).get());

    /* Serialize and deserialize into a byte array (descending sort,
     * with two reserved bits set to 0x3)
     */
    i.setReservedBits(2).setReservedValue(0x3).setOrder(Order.DESCENDING);
    w.set(0);
    System.out.println("deserialize(serialize(0)) = " + 
        ((IntWritable)i.deserialize(i.serialize(w))).get());

    /* Serialize and deserialize NULL into a byte array */
    System.out.println("deserialize(serialize(NULL)) = " + 
        i.deserialize(i.serialize(null)));
  }

  public static void main(String[] args) throws Exception {
    IntExample e = new IntExample();
    e.lengthExamples();
    e.serializationExamples();
  }
}
