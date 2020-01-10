/*
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
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.iapi.stats;

import java.io.Externalizable;
import java.io.Serializable;
import java.util.Comparator;

/**
 *
 * Interface for a statistical item.
 *
 */
public interface ItemStatistics<T extends Comparator<T>> extends Serializable, Externalizable {
    /**
     *
     * Enumeration of statistical types.  NONUNIQUEKEY,UNIQUEKEY,PRIMARYKEY todo.
     *
     * @return
     */
    enum Type {
        COLUMN,NONUNIQUEKEY,UNIQUEKEY,PRIMARYKEY,FAKE
    }

    /**
     *
     * Statistical type.
     *
     * @return
     */
    Type getType();
    /**
     *
     * Retrieve typed minimum value
     *
     * @return
     */
    T minValue();
    /**
     *
     * Retrieve typed maximum value
     *
     * @return
     */
    T maxValue();
    /**
     *
     * Return total count
     *
     * @return
     */
    long totalCount();
    /**
     *
     * Return null count
     *
     * @return
     */
    long nullCount();
    /**
     *
     * Return not null count
     *
     * @return
     */
    long notNullCount();
    /**
     *
     * Return cardinality (number of unique values)
     *
     * @return
     */
    long cardinality();

    /**
     * @param element the element to match
     * @return the number of entries which are <em>equal</em> to the specified element.
     */
    long selectivity(T element);

    /**
     * @param start the start of the range to estimate. If {@code null}, then scan everything before {@code stop}.
     *              If {@code stop} is also {@code null}, then this will return an estimate to the number of entries
     *              in the entire data set.
     * @param stop the end of the range to estimate. If {@code null}, then scan everything after {@code start}.
     *             If {@code start} is also {@code null}, then this will return an estimate of the number of entries
     *             in the entire data set.
     * @param includeStart if {@code true}, then include entries which are equal to {@code start}
     * @param includeStop if {@code true}, then include entries which are <em>equal</em> to {@code stop}
     * @param useExtrapolation if {@code true}, then do extrapolation if the range falls beyond the min-max range recorded in stats
     * @return the number of rows which fall in the range {@code start},{@code stop}, with
     * inclusion determined by {@code includeStart} and {@code includeStop}
     */
    long rangeSelectivity(T start,T stop, boolean includeStart,boolean includeStop,boolean useExtrapolation);

    long rangeSelectivity(T start,T stop, boolean includeStart,boolean includeStop);

    /**
     *
     * Update the statistics by applying the typed item.
     *
     * @param item
     */
    void update(T item);
    /**
     *
     * Clone the statistics
     *
     * @return
     */
    ItemStatistics<T> getClone();

    /**
     * get the row count excluding the specified value
     * @param value
     * @return
     */
    long selectivityExcludingValueIfSkewed(T value);

}
