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

import orderly.AbstractVarIntRowKey;
import orderly.RowKey;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import static org.junit.Assert.assertEquals;

public abstract class AbstractVarIntRowKeyTestCase extends RandomRowKeyTestCase
{
  protected int reservedBits, reservedValue;
  protected AbstractVarIntRowKey vi;

  public abstract AbstractVarIntRowKey createVarIntRowKey();

  @Override
  public RowKey createRowKey() {
    vi = createVarIntRowKey();
    reservedBits = r.nextInt(vi.getMaxReservedBits());
    reservedValue = r.nextInt(1 << reservedBits);
    return vi.setReservedBits(reservedBits)
             .setReservedValue(reservedValue);
  }

  protected void verifyReserved(ImmutableBytesWritable w) {
    byte[] b = w.get();
    int offset = w.getOffset();

    int reservedActual = (b[offset] & 0xff) >>> Byte.SIZE - reservedBits;
    assertEquals("Reserved bits corrupt", reservedValue, reservedActual);
  }

  @Override
  public void testSerialization(Object o, ImmutableBytesWritable w) 
    throws IOException 
  {
    super.testSerialization(o, w);
    if (o != null || vi.terminate())
      verifyReserved(w);
  }

  @Override
  public void testSkip(Object o, ImmutableBytesWritable w) 
    throws IOException
  {
    super.testSkip(o, w);
    if (o != null || vi.terminate())
      verifyReserved(w);
  }

  @Override
  public void testSort(Object o1, ImmutableBytesWritable w1, Object o2, 
      ImmutableBytesWritable w2) throws IOException
  {
    super.testSort(o1, w1, o2, w2);
    if (o1 != null || vi.terminate())
      verifyReserved(w1);
    if (o2 != null || vi.terminate())
      verifyReserved(w2);
  }

  @Override
  public int compareTo(Object o1, Object o2) {
    if (o1 == null || o2 == null)
      return (o1 != null ? 1 : 0) - (o2 != null ? 1 : 0);

    long x = vi.getWritable((Writable)o1),
         y = vi.getWritable((Writable)o2);

    /* Sign bits may be implicit (not present in x/y) or explicit (stored 
     * in the most significant bit of x/y using two's complement encoding). We 
     * compare the sign bits first, and then only compare x and y if the sign 
     * bits are equal.
     */
    if (vi.getSign(x) != vi.getSign(y))
        return (vi.getSign(x) == 0 ? 1 : 0) - (vi.getSign(y) == 0 ? 1 : 0);

    /* The Java long comparison operator interprets the most significant bit of
     * x/y as the sign bit, but in implicit signed representations the most 
     * significant bit is data. We flip the most significant bit of x/y with an
     * XOR so that data will compare correctly when using Java long comparison. 
     * This bit flip has no effect on the comparison if x and y use an explicit
     * sign bit because the sign bits are equal, and inverting two equal values
     * does not affect the comparison result.
     */ 
    x ^= Long.MIN_VALUE;
    y ^= Long.MIN_VALUE;
    return (x > y ? 1 : 0) - (y > x ? 1 : 0);
  }
}
