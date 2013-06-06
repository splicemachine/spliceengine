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

import java.math.BigDecimal;


import orderly.BigDecimalRowKey;
import orderly.LazyBigDecimalRowKey;
import orderly.Order;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public class BigDecimalExample
{
  /* Simple examples showing serialization lengths with BigDecimalRow Key */
  public void lengthExamples() throws Exception {
    BigDecimalRowKey i = new BigDecimalRowKey();

    System.out.println("serialize(null) length - " + i.serialize(null).length);
    System.out.println("serialize(58.75) length - " + 
        i.serialize(new BigDecimal(58.75)).length);
    System.out.println("serialize(107-e902) length - " + 
        i.serialize(new BigDecimal("107e-902")).length);

    i.setOrder(Order.DESCENDING);
    System.out.println("descending serialize (null) - length " + 
        i.serialize(null).length);
    System.out.println("descending serialize (57) - length " + 
        i.serialize(new BigDecimal(57)).length);
  }

  /* Simple examples showing serialization tests with BD/LazyBigDecimal */
  public void serializationExamples() throws Exception {
    BigDecimalRowKey i = new BigDecimalRowKey();
    LazyBigDecimalRowKey l = new LazyBigDecimalRowKey();
    ImmutableBytesWritable buffer = new ImmutableBytesWritable();
    byte[] b;
    BigDecimal bd;

    /* Serialize and deserialize into an immutablebyteswritable */
    bd = new BigDecimal("107e-902");
    b = new byte[i.getSerializedLength(bd)];
    buffer.set(b);
    i.serialize(bd, buffer);
    buffer.set(b, 0, b.length);
    System.out.println("deserialize(serialize(107e-902)) = " + 
        i.deserialize(buffer));

    /* Serialize and deserialize into a byte array (descending sort)
     * using lazybigdecimal
     */
    l.setOrder(Order.DESCENDING);
    System.out.println("deserialize(serialize(0)) = " + 
        l.getBigDecimal(
          (ImmutableBytesWritable)l.deserialize(l.serialize(BigDecimal.ZERO))));

    /* Serialize and deserialize NULL into a byte array */
    System.out.println("deserialize(serialize(NULL)) = " + 
        i.deserialize(i.serialize(null)));
  }

  public static void main(String[] args) throws Exception {
    BigDecimalExample e = new BigDecimalExample();
    e.lengthExamples();
    e.serializationExamples();
  }
}
