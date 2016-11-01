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
package com.splicemachine.db.iapi.stats;

import java.util.Comparator;
import java.util.List;

/**
 * Representation of Partition-level statistics.
 *
 * Partition-level statistics are statistics aboud the partition itself--how many rows,
 * the width of those rows, and some physical statistics about that. These
 * partitions are designed to be mergeable into either a server-level, or a table-level
 * view of that data.
 *
 * @author Scott Fines
 *         Date: 2/23/15
 */
public interface PartitionStatistics {
    /**
     * @return the total number of rows in the partition.
     */
    long rowCount();
    /**
     * @return the total size of the partition (in bytes).
     */
    long totalSize();

    /**
     * @return the average width of a single row (in bytes) in this partition. This includes
     * the row key and cell contents.
     */
    int avgRowWidth();
    /**
     * @return a unique identifier for this partition
     */
    String partitionId();

    /**
     * @return Statistics about individual columns (which were most recently collected).
     */
    List<? extends ItemStatistics> getAllColumnStatistics();


    <T extends Comparator<T>> T minValue(int positionNumber);

    <T extends Comparator<T>> T maxValue(int positionNumber);

    long nullCount(int positionNumber);

    long notNullCount(int positionNumber);

    long cardinality(int positionNumber);

    /**
     * @param element the element to match
     * @return the number of entries which are <em>equal</em> to the specified element.
     */
    <T extends Comparator<T>> long selectivity(T element,int positionNumber);

    /**
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
    <T extends Comparator<T>> long rangeSelectivity(T start,T stop, boolean includeStart,boolean includeStop,int positionNumber);

    /**
     * @param columnId the identifier of the column to fetch(indexed from 0)
     * @return statistics for the column, if such statistics exist, or {@code null} if
     * no statistics are available for that column.
     */
    ItemStatistics getColumnStatistics(int columnId);

}

