/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.impl.sql.compile;

import static org.sparkproject.guava.base.Preconditions.checkArgument;

public class CostUtils {

    /**
     * In cost calculations we don't want to overflow and a have large positive row count turn into a negative count.
     * This method assumes that for our purposes any count greater than Long.MAX_VALUE can be approximated as
     * Long.MAX_VALUE.We don't have the same consideration for cost as double overflow results in a value of
     * Double.POSITIVE_INFINITY which behaves correctly (to my knowledge) in our cost calculations.
     */
    public static long add(long count1, long count2) {
        checkArgument(count1 >= 0);
        checkArgument(count2 >= 0);
        long sum = count1 + count2;
        return (sum >= 0L) ? sum : Long.MAX_VALUE;
    }

}
