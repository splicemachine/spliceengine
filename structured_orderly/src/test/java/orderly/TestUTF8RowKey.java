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

import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;

public class TestUTF8RowKey extends RandomRowKeyTestCase
{
  protected int maxLength;

  @Before
  @Override
  public void setUp() {
    super.setUp();
    maxLength = Integer.valueOf(System.getProperty("test.random.maxstrlength", 
        "1024"));
  }

  @Override
  public RowKey createRowKey() { return new UTF8RowKey(); }

  @Override
  public Object createObject() {
    if (r.nextInt(128) == 0)
      return null;

    int len = r.nextInt(maxLength);
    StringBuilder sb = new StringBuilder(len);

    for (int i = 0; i < len; i++) 
      sb.appendCodePoint(r.nextInt(Character.MAX_CODE_POINT + 1));
    return Bytes.toBytes(sb.toString());
  }

  @Override
  public int compareTo(Object o1, Object o2) {
    if (o1 == null || o2 == null)
      return (o1 != null ? 1 : 0) - (o2 != null ? 1 : 0);
    return Integer.signum(Bytes.compareTo((byte[])o1, (byte[])o2));
  }
}
