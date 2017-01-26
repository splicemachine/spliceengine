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
 *
 * This class unions the existing partition statistics to create a single
 * effective partition.  The rationale for this is to not merge the stats together
 * continually nor apply them in a linear fashion to each partition.
 *
 *
 *
 */
public class EffectivePartitionStatisticsImpl implements PartitionStatistics {
    private ItemStatistics[] itemStatistics;
    private long rowCount;
    private long totalSize;
    private int avgRowWidth;
    double fallbackNullFraction;
    double extraQualifierMultiplier;


    public EffectivePartitionStatisticsImpl() {

    }

    /**
     *
     * Generating effective partitions from the merges of columns.
     *
     * @param itemStatisticsBuilder
     * @param rowCount
     * @param totalSize
     * @param avgRowWidth
     */
    public EffectivePartitionStatisticsImpl(ColumnStatisticsMerge[] itemStatisticsBuilder,
                                            long rowCount, long totalSize,
                                            int avgRowWidth,
                                            double fallbackNullFraction,
                                                    double extraQualifierMultiplier) {
        this.rowCount = rowCount;
        this.totalSize = totalSize;
        this.avgRowWidth = avgRowWidth;
        this.fallbackNullFraction = fallbackNullFraction;
        this.extraQualifierMultiplier = extraQualifierMultiplier;
       itemStatistics = new ItemStatistics[itemStatisticsBuilder.length];
       for (int i =0; i<itemStatisticsBuilder.length;i++) {
            itemStatistics[i] = itemStatisticsBuilder[i].terminate();
       }
    }

    /**
     * Row Count
     *
     * @return
     */
    @Override
    public long rowCount() {
        return rowCount;
    }

    /**
     *
     * Return total size
     *
     * @return
     */
    @Override
    public long totalSize() {
        return totalSize;
    }

    /**
     *
     * Return average row width
     *
     * @return
     */
    @Override
    public int avgRowWidth() {
        return avgRowWidth;
    }

    /**
     *
     * No Partition associated with an effective partition.
     *
     * @return
     */
    @Override
    public String partitionId() {
        return null;
    }

    /**
     *
     * Unsupported operation.
     *
     * @return
     */
    @Override
    public List<? extends ItemStatistics> getAllColumnStatistics() {
        throw new UnsupportedOperationException("Use getAllColumnStatistics on the table vs. agains the effective partition.");
    }

    /**
     *
     * This is 0 based retrieval of statistics.
     *
     * @param columnId the identifier of the column to fetch(indexed from 0)
     * @return
     */
    @Override
    public ItemStatistics getColumnStatistics(int columnId) {
        return columnId >= itemStatistics.length?null:itemStatistics[columnId];
    }

    /**
     *
     * Grab the minimum value from the statistics on the column.
     *
     * @param positionNumber
     * @param <T>
     * @return
     */
    @Override
    public <T extends Comparator<T>> T minValue(int positionNumber) {
        ItemStatistics stats = positionNumber >= itemStatistics.length?null:itemStatistics[positionNumber];
        return (T) (stats==null?null:itemStatistics[positionNumber].minValue());
    }

    /**
     *
     * Grab the maximum value from the statistics on the column.
     *
     * @param positionNumber
     * @param <T>
     * @return
     */
    @Override
    public <T extends Comparator<T>> T maxValue(int positionNumber) {
        ItemStatistics stats = positionNumber >= itemStatistics.length?null:itemStatistics[positionNumber];
        return (T) (stats==null?null:itemStatistics[positionNumber].maxValue());
    }

    /**
     *
     * Grab the null count from the statistics for the column.
     *
     * @param positionNumber
     * @return
     */
    @Override
    public long nullCount(int positionNumber) {
        ItemStatistics stats = positionNumber >= itemStatistics.length?null:itemStatistics[positionNumber];
        return stats==null?(long) (fallbackNullFraction * (double) rowCount()):itemStatistics[positionNumber].nullCount();
    }

    /**
     *
     * Grab the null count from the statistics for the column.
     *
     * @param positionNumber
     * @return
     */
    @Override
    public long notNullCount(int positionNumber) {
        ItemStatistics stats = positionNumber >= itemStatistics.length?null:itemStatistics[positionNumber];
        return stats==null?(long) ( (1.0 - fallbackNullFraction) * (double) rowCount()):itemStatistics[positionNumber].notNullCount();
    }

    /**
     *
     * Grab the cardinality from the statistics for the column.
     *
     * @param positionNumber
     * @return
     */
    @Override
    public long cardinality(int positionNumber) {
        ItemStatistics stats = positionNumber >= itemStatistics.length?null:itemStatistics[positionNumber];
        return stats==null?rowCount():itemStatistics[positionNumber].cardinality();
    }

    /**
     *
     * Grab the selectivity from the statistics for the column.
     *
     * @param element the element to match
     * @param positionNumber
     * @param <T>
     * @return
     */
    @Override
    public <T extends Comparator<T>> long selectivity(T element, int positionNumber) {
        ItemStatistics stats = positionNumber >= itemStatistics.length?null:itemStatistics[positionNumber];
        return stats==null?(long) (( (double) rowCount()) * extraQualifierMultiplier ):itemStatistics[positionNumber].selectivity((T) element);
    }

    /**
     *
     * Unsupported Range Selectivity on effective partitions.  We expect the
     * developer to calculate range selectivity on each partition.  Depending
     * on CPU usage, we may want to revisit.
     *
     * @param start the start of the range to estimate. If {@code null}, then scan everything before {@code stop}.
     *              If {@code stop} is also {@code null}, then this will return an estimate to the number of entries
     *              in the entire data set.
     * @param stop the end of the range to estimate. If {@code null}, then scan everything after {@code start}.
     *             If {@code start} is also {@code null}, then this will return an estimate of the number of entries
     *             in the entire data set.
     * @param includeStart if {@code true}, then include entries which are equal to {@code start}
     * @param includeStop if {@code true}, then include entries which are <em>equal</em> to {@code stop}
     * @param positionNumber
     * @param <T>
     * @return
     */
    @Override
    public <T extends Comparator<T>> long rangeSelectivity(T start, T stop, boolean includeStart, boolean includeStop, int positionNumber) {
        throw new UnsupportedOperationException("Use Range Selectivity on the table vs. agains the effective partition.");
    }
}
