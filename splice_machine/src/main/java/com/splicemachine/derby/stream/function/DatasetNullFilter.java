/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
 *
 */

package com.splicemachine.derby.stream.function;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

public class DatasetNullFilter<V> implements FilterFunction<V> {
    private final int[] hashKeys;

    public DatasetNullFilter(int[] hashKeys) {
        this.hashKeys = hashKeys;
    }

    @Override
    public boolean call(V value) throws Exception {
        Row r = (Row) value;
        for (int key : hashKeys) {
            if (r.isNullAt(key))
                return true;
        }
        return false;
    }
}
