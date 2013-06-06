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

import orderly.FloatWritableRowKey;
import orderly.RowKey;

import org.apache.hadoop.io.FloatWritable;

public class TestFloatWritableRowKey extends RandomRowKeyTestCase
{
  @Override
  public RowKey createRowKey() { return new FloatWritableRowKey(); }

  @Override
  public Object createObject() {
    if (r.nextInt(128) == 0)
      return null;

    float f;
    switch (r.nextInt(128)) {
      case 0:
        f = +0.0f;
        break;

      case 1:
        f = -0.0f;
        break;

      case 2:
        f = Float.POSITIVE_INFINITY;
        break;

      case 3:
        f = Float.NEGATIVE_INFINITY;
        break;

      case 4:
        f = Float.NaN;
        break;

      default:
        f = r.nextFloat();
        break;
    }

    return new FloatWritable(f);
  }

  private boolean isPositiveZero(float f) {
    return 1/f == Float.POSITIVE_INFINITY;
  }

  @Override
  public int compareTo(Object o1, Object o2) {
    if (o1 == null || o2 == null)
      return (o1 != null ? 1 : 0) - (o2 != null ? 1 : 0);

    float f = ((FloatWritable)o1).get(),
          g = ((FloatWritable)o2).get();

    if (!Float.isNaN(f) && !Float.isNaN(g) && !(f == 0 && g == 0)) 
      return ((f > g) ? 1 : 0) - ((g > f) ? 1 : 0);

    if (Float.isNaN(f)) {
      if (Float.isNaN(g))
        return 0;
      return 1;
    } else if (Float.isNaN(g)) {
      return -1;
    } else /* f == +/-0.0 && g == +/-0.0 */ {
      return (isPositiveZero(f) ? 1 : 0) - (isPositiveZero(g) ? 1 : 0);
    }
  }
}
