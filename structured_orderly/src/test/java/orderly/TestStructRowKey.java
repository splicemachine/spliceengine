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
import java.util.ArrayList;
import java.util.List;

import orderly.Order;
import orderly.RowKey;
import orderly.StructIterator;
import orderly.StructRowKey;

import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import org.junit.After;
import org.junit.Before;

public class TestStructRowKey extends RandomRowKeyTestCase
{
  protected static final List<Class<? extends RandomRowKeyTestCase>> 
    TESTS_PRIMITIVE, TESTS_ALL;

  static {
    List<Class<? extends RandomRowKeyTestCase>> prim = 
      new ArrayList<Class<? extends RandomRowKeyTestCase>>();
    prim.add(TestBigDecimalRowKey.class);
    prim.add(TestDoubleRowKey.class);
    prim.add(TestDoubleWritableRowKey.class);
    prim.add(TestFixedIntegerRowKey.class);
    prim.add(TestFixedIntWritableRowKey.class);
    prim.add(TestFixedLongRowKey.class);
    prim.add(TestFixedLongWritableRowKey.class);
    prim.add(TestFixedUnsignedIntegerRowKey.class);
    prim.add(TestFixedUnsignedIntWritableRowKey.class);
    prim.add(TestFixedUnsignedLongRowKey.class);
    prim.add(TestFixedUnsignedLongWritableRowKey.class);
    prim.add(TestFloatRowKey.class);
    prim.add(TestFloatWritableRowKey.class);
    prim.add(TestIntegerRowKey.class);
    prim.add(TestIntWritableRowKey.class);
    prim.add(TestLazyBigDecimalRowKey.class);
    prim.add(TestLongRowKey.class);
    prim.add(TestLongWritableRowKey.class);
    prim.add(TestStringRowKey.class);
    prim.add(TestTextRowKey.class);
    prim.add(TestUnsignedIntegerRowKey.class);
    prim.add(TestUnsignedIntWritableRowKey.class);
    prim.add(TestUnsignedLongRowKey.class);
    prim.add(TestUnsignedLongWritableRowKey.class);
    prim.add(TestUTF8RowKey.class);
    TESTS_PRIMITIVE = prim;

    List<Class<? extends RandomRowKeyTestCase>> all = 
      new ArrayList<Class<? extends RandomRowKeyTestCase>>(prim);
    all.add(TestStructRowKey.class);
    TESTS_ALL = all;
  }

  protected int maxFields, maxNest;
  protected RandomRowKeyTestCase[] fieldTests;

  public TestStructRowKey() { maxNest = -1; }

  public TestStructRowKey setMaxNest(int maxNest) {
    this.maxNest = maxNest;
    return this;
  }

  protected RandomRowKeyTestCase randField() {
    List<Class<? extends RandomRowKeyTestCase>> cList = 
      maxNest > 0 ? TESTS_ALL : TESTS_PRIMITIVE;

    RandomRowKeyTestCase t;
    try { 
      t = cList.get(r.nextInt(cList.size())).newInstance();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    if (t instanceof TestStructRowKey) 
      ((TestStructRowKey)t).setMaxNest(maxNest - 1);
    t.setRandom(r);
    t.setUp();
    return t;
  }

  @Before
  @Override
  public void setUp() {
    super.setUp();
    maxFields = Integer.valueOf(System.getProperty("test.random.maxfieldcount", 
        "16"));
    if (maxNest < 0)
      maxNest = Integer.valueOf(System.getProperty("test.random.maxfieldnest", 
            "4"));

    fieldTests = new RandomRowKeyTestCase[r.nextInt(maxFields)];
    for (int i = 0; i < fieldTests.length; i++)
      fieldTests[i] = randField();
  }

  @After
  @Override
  public void tearDown() {
    for (int i = 0; i < fieldTests.length; i++) {
      fieldTests[i].tearDown();
      fieldTests[i] = null;
    }

    super.tearDown();
  }

  @Override
  public RowKey createRowKey() {
    RowKey[] fields = new RowKey[fieldTests.length];
    for (int i = 0; i < fields.length; i++) {
      fields[i] = fieldTests[i].createRowKey();
      fields[i].setOrder(r.nextBoolean() ? Order.ASCENDING : Order.DESCENDING);
      fieldTests[i].setRowKey(fields[i]);
    }
    return new StructRowKey(fields);
  }

  @Override
  public Object createObject() {
    Object[] o = new Object[fieldTests.length];
    for (int i = 0; i < o.length; i++)
      o[i] = fieldTests[i].createObject();
    return o;
  }

  @Override
  public Object deserialize(ImmutableBytesWritable w) throws IOException 
  {
    if (r.nextInt(64) != 0) 
      return super.deserialize(w);

    Object[] o = new Object[fieldTests.length];
    StructIterator iter = ((StructRowKey)key).iterateOver(w).iterator();

    int pos = 0;
    while (iter.hasNext()) o[pos++] = iter.next(); 
    return o;
  }

  @Override
  public int compareTo(Object o1, Object o2) {
    Object[] f1 = (Object[]) o1,
             f2 = (Object[]) o2;

    if (f1.length != f2.length)
      throw new IndexOutOfBoundsException("Comparing fields with length " +
          f1.length + " to fields with length " + f2.length);

    for (int i = 0; i < f1.length; i++) {
      int r = fieldTests[i].compareTo(f1[i], f2[i]);
      if (r != 0) {
        if (fieldTests[i].getRowKey().getOrder() == Order.DESCENDING) 
          r = -r;
        if (getRowKey().getOrder() == Order.DESCENDING)
          r = -r;
        return r;
      }
    }

    return 0;
  }
}
