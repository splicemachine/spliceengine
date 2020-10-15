/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.compile;

import static splice.com.google.common.base.Preconditions.checkArgument;

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
