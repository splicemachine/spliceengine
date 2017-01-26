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
 * All such Splice Machine modifications are Copyright 2012 - 2017 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.iapi.stats;

import java.util.Comparator;
import java.util.List;

/**
 * Representation of Table-level statistics.
 *
 * Table statistics are statistics about the overall table itself, accumulated over all partitions.
 *
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface TableStatistics {
    /**
     *
     * The unique identifier for the table
     *
     * @return
     */
    public String tableId();

    /**
     * @return the total number of entries in the table across all partitions
     */
    long rowCount();

    /**
     * @return the average width of a single row (in bytes) across all partitions. This includes
     * the row key and cell contents.
     */
    int avgRowWidth();

    /**
     * @return Detailed statistics for each partition.
     */
    List<? extends PartitionStatistics> getPartitionStatistics();

    /**
     *
     * Effective Partition Statistics for the entire table.
     *
     * @return
     */
    PartitionStatistics getEffectivePartitionStatistics();

    /**
     *
     * MinimumValue in the Table
     *
     * @param positionNumber
     * @param <T>
     * @return
     */
    <T extends Comparator<T>> T minValue(int positionNumber);
    /**
     *
     * MaximumValue in the Table
     *
     * @param positionNumber
     * @param <T>
     * @return
     */
    <T extends Comparator<T>> T maxValue(int positionNumber);
    /**
     *
     * Null Count of the Position (0 based)
     *
     * @param positionNumber
     * @return long
     */
    long nullCount(int positionNumber);
    /**
     *
     * Not Null Count of the Position (0 based)
     *
     * @param positionNumber
     * @return long
     */
    long notNullCount(int positionNumber);
    /**
     *
     * Cardinality of the Position (0 based)
     *
     * @param positionNumber
     * @return long
     */
    long cardinality(int positionNumber);
    /**
     *
     * Number of Partitions
     *
     * @return int
     */
    int numPartitions();

    /**
     *
     * Selectivity of a single element.
     *
     * @param element the element to match
     * @return the number of entries which are <em>equal</em> to the specified element.
     */
    <T extends Comparator<T>> double selectivity(T element,int positionNumber);

    /**
     *
     * Range Selectivity calculation for an entire table.  Implementations should be smart enough
     * to take partitions into account.
     *
     * @param start the start of the range to estimate. If {@code null}, then scan everything before {@code stop}.
     *              If {@code stop} is also {@code null}, then this will return an estimate to the number of entries
     *              in the entire data set.
     * @param stop the end of the range to estimate. If {@code null}, then scan everything after {@code start}.
     *             If {@code start} is also {@code null}, then this will return an estimate of the number of entries
     *             in the entire data set.
     * @param includeStart if {@code true}, then include entries which are equal to {@code start}
     * @param includeStop if {@code true}, then include entries which are <em>equal</em> to {@code stop}
     * @return the number of rows which fall in the range {@code start},{@code stop}, with
     * inclusion determined by {@code includeStart} and {@code includeStop}
     */
    <T extends Comparator<T>> double rangeSelectivity(T start,T stop, boolean includeStart,boolean includeStop,int positionNumber);

}

