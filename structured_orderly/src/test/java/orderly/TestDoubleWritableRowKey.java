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

import orderly.DoubleWritableRowKey;
import orderly.RowKey;

import org.apache.hadoop.io.DoubleWritable;

public class TestDoubleWritableRowKey extends RandomRowKeyTestCase
{
  @Override
  public RowKey createRowKey() { return new DoubleWritableRowKey(); }

  @Override
  public Object createObject() {
    if (r.nextInt(128) == 0)
      return null;

    double d;
    switch (r.nextInt(128)) {
      case 0:
        d = +0.0d;
        break;

      case 1:
        d = -0.0d;
        break;

      case 2:
        d = Double.POSITIVE_INFINITY;
        break;

      case 3:
        d = Double.NEGATIVE_INFINITY;
        break;

      case 4:
        d = Double.NaN;
        break;

      default:
        d = r.nextDouble();
        break;
    }

    return new DoubleWritable(d);
  }

  private boolean isPositiveZero(double d) {
    return 1/d == Double.POSITIVE_INFINITY;
  }

  @Override
  public int compareTo(Object o1, Object o2) {
    if (o1 == null || o2 == null)
      return (o1 != null ? 1 : 0) - (o2 != null ? 1 : 0);

    double d = ((DoubleWritable)o1).get(),
           e = ((DoubleWritable)o2).get();

    if (!Double.isNaN(d) && !Double.isNaN(e) && !(d == 0 && e == 0)) 
      return ((d > e) ? 1 : 0) - ((e > d) ? 1 : 0);

    if (Double.isNaN(d)) {
      if (Double.isNaN(e))
        return 0;
      return 1;
    } else if (Double.isNaN(e)) {
      return -1;
    } else /* d == +/-0.0 && e == +/-0.0 */ {
      return (isPositiveZero(d) ? 1 : 0) - (isPositiveZero(e) ? 1 : 0);
    }
  }
}
