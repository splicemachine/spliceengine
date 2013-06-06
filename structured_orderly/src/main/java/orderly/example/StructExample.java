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
import orderly.DoubleRowKey;
import orderly.FixedUnsignedLongRowKey;
import orderly.IntegerRowKey;
import orderly.Order;
import orderly.RowKey;
import orderly.StringRowKey;
import orderly.StructBuilder;
import orderly.StructIterator;
import orderly.StructRowKey;
import orderly.Termination;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

public class StructExample
{
  BigDecimalRowKey bd;
  DoubleRowKey d;
  FixedUnsignedLongRowKey ul;
  StringRowKey s;
  IntegerRowKey i;

  public StructExample() {
    bd = new BigDecimalRowKey();
    ul = new FixedUnsignedLongRowKey();
    s = new StringRowKey();
    d = new DoubleRowKey();
    i = new IntegerRowKey();
  }

  public void structLengthExample() throws Exception {
    StructRowKey fin = new StructRowKey(new RowKey[] { i, bd, s });
    System.out.println("struct(null) length - " + 
        fin.serialize(new Object[] { null, null, null }).length);

    /* Show descending sort */
    fin.setOrder(Order.DESCENDING);
    System.out.println("struct DESC (null) length - " + 
        fin.serialize(new Object[] { null, null, null }).length);

    /* Back to ascending */
    fin.setOrder(Order.ASCENDING);
    System.out.println("struct (293, 2934, hi) length - " + 
        fin.serialize(new Object[] { 293, new BigDecimal("2934"), "hi" }).length);
    System.out.println("struct (293, 2934, null) length - " + 
        fin.serialize(new Object[] { 293, new BigDecimal("2934"), null }).length);

    /* Force termination */
    fin.setTermination(Termination.MUST);
    System.out.println("mustTerminate struct (293, 2934, hi) length - " + 
        fin.serialize(new Object[] { 293, new BigDecimal("2934"), "hi" }).length);
    System.out.println("mustTerminate struct (293, 2934, null) length - " + 
        fin.serialize(new Object[] { 293, new BigDecimal("2934"), null }).length);
    fin.setTermination(Termination.SHOULD_NOT);

    fin.setOrder(Order.DESCENDING);
    System.out.println("struct DESC (293, 2934, hi) length - " + 
        fin.serialize(new Object[] { 293, new BigDecimal("2934"), "hi" }).length);
    System.out.println("struct DESC (293, 2934, null) length - " + 
        fin.serialize(new Object[] { 293, new BigDecimal("2934"), null }).length);
    fin.setOrder(Order.ASCENDING);
  }

  void printStruct(Object obj, String prefix) {
    Object[] o = (Object[]) obj;
    if (o == null) {
      System.out.println(prefix + "Struct: NULL");
      return;
    }

    System.out.println(prefix + "Struct: ");
    int pos = 0;
    for (Object field : o) {
      if (field instanceof Object[]) {
        System.out.println(prefix + " Field " + pos++ + " nested: ");
        printStruct(field, prefix + "\t");
      } else {
        System.out.println(prefix + "  Field " + pos++ + " = " + field);
      }
    }
  }

  void printStruct(Object obj) { printStruct(obj, ""); }

  public void structSerializationExample() throws Exception {
    StructRowKey r = new StructRowKey(new RowKey[] { s, bd });

    /* Serialize, deserialize using byte array */
    System.out.println("deserialize(serialize(foobarbaz, 3.14159e102))");
    printStruct(r.deserialize(r.serialize(new Object[] { "foobarbaz", new
          BigDecimal(3.14159e102)})));

    /* Serialize, deserialize using ImmutableBytesWritable */
    ImmutableBytesWritable buffer = new ImmutableBytesWritable();
    Object[] o = new Object[] { "helloworld", null };
    byte[] b = new byte[r.getSerializedLength(o)];
    buffer.set(b);
    r.serialize(o, buffer);
    buffer.set(b);
    System.out.println("deserialize(serialize(helloworld, null))");
    printStruct(r.deserialize(buffer));
  }

  public void prefixExample() throws Exception {
    /* Store a row using (string, double, bigdecimal), but retrieve it using
     * (string, double) and just (string)
     */
    StructRowKey rk = new StructRowKey(new RowKey[] { s, d, bd }),
                 prefix1 = new StructRowKey(new RowKey[] { s, d}),
                 prefix2 = new StructRowKey(new RowKey[] { s });

    byte[] b = rk.serialize(new Object[] { "hello", 3.14159, 
        new BigDecimal("0.93e-102") });

    System.out.println("Deserialize first two fields of " +
        "(hello, 3.14159, 0.93e-102)");
    printStruct(prefix1.deserialize(b));

    System.out.println("Deserialize first field of " +
        "(hello, 3.14159, 0.93e-102)");
    printStruct(prefix2.deserialize(b));

    System.out.println("Deserialize all fields of " +
        "(hello, 3.14159, 0.93e-102)");
    printStruct(rk.deserialize(b));
  }

  public void builderAndIteratorExample() throws Exception {
    StructRowKey rk = new StructBuilder().add(s).add(d).add(ul).toRowKey();
    Object[] o = new Object[] { "hello", 3.14159, 17L };
    StructIterator iterator = rk.iterateOver(rk.serialize(o)).iterator();

    /* Use the deserialize, skip classes to selectively deserialze objects */
    System.out.println("Printing fields 1, 3 of (hello, 3.14159, 17");
    System.out.println(iterator.deserialize());
    iterator.skip();
    System.out.println(iterator.deserialize());

    /* Just treat StructRowKey as an iterable -- iterates over all row key 
     * fields, deserializing each object in succession
     */
    System.out.println("Printing fields all fields (hello, 3.14159, 17");
    for (Object field : rk)
      System.out.println(field);
  }

  public void nestedStructExample() throws Exception {
    StructBuilder b = new StructBuilder();
    StructRowKey n = b.add(ul).add(bd).toRowKey(),
                 rk = b.reset().add(s).add(n).add(i).toRowKey();
    System.out.println("Serializing (outerString, (17, 940.2e-87)), 42");
    Object[] o = new Object[] { "outerString", 
      new Object[] { 17L, new BigDecimal("940.2e-87") }, 42};
    byte[] a = rk.serialize(o);
    System.out.println("Length " + a.length);
    printStruct(rk.deserialize(a));
  }

  public static void main(String[] args) throws Exception {
    StructExample e = new StructExample();
    e.structLengthExample();
    e.structSerializationExample();
    e.prefixExample();
    e.builderAndIteratorExample();
    e.nestedStructExample();
  }
}
