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

import java.math.BigDecimal;
import java.math.BigInteger;

import orderly.BigDecimalRowKey;
import orderly.RowKey;

import org.junit.Before;

public class TestBigDecimalRowKey extends RandomRowKeyTestCase
{
  protected int maxBits;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    maxBits = Integer.valueOf(System.getProperty("test.random.maxbits", 
          "1024"));
  }

  @Override
  public RowKey createRowKey() { return new BigDecimalRowKey(); }

  private BigInteger randBigInteger() {
    int bits = r.nextInt(maxBits);
    switch (r.nextInt(128)) {
      case 0:
        bits &= 63;
      case 1:
        bits &= 65535;
      case 2:
        bits &= ((1 << 21) - 1);
    }

    BigInteger i = new BigInteger(bits, r);
    if (r.nextBoolean())
      i = i.negate();
    return i;
  }

  private int randScale(int unscaledBits) {
    int scale = r.nextInt(Integer.MAX_VALUE - unscaledBits);
    if (r.nextBoolean()) scale = -scale;

    switch (r.nextInt(128)) {
      case 0: 
        scale = (scale & 127) - 64;
        break;

      case 1:
        scale = (scale & 16383) - 8192;
        break;

      case 2:
        scale = (scale & ((1 << 21) - 1)) - (1 << 20);
        break;
    }

    return scale;
  }

  @Override
  public Object createObject() {
    if (r.nextInt(128) == 0)
      return null;

    BigInteger i = randBigInteger();
    int scale = randScale(i.bitCount());
    return new BigDecimal(i, scale);
  }

  @Override
  public int compareTo(Object o1, Object o2) {
    if (o1 == null || o2 == null)
      return (o1 != null ? 1 : 0) - (o2 != null ? 1 : 0);
    return ((BigDecimal)o1).compareTo((BigDecimal)o2);
  }
}
