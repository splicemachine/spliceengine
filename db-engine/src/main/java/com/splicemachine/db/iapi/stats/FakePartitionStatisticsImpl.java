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

import com.splicemachine.db.iapi.sql.dictionary.TableDescriptor;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 *
 * This is a stubbed out partion implementation where we do not have statistical information.
 *
 *
 */
public class FakePartitionStatisticsImpl implements PartitionStatistics {
//    PartitionStatisticsDescriptor partitionStatistics;
    private List<? extends ItemStatistics> itemStatistics = Collections.EMPTY_LIST;
    String tableId;
    String partitionId;
    long numRows;
    long heapSize;
    double fallbackNullFraction;
    double extraQualifierMultiplier;

    /**
     *
     * Create the stubbed partition statistics.
     *
     * @param tableId
     * @param partitionId
     * @param numRows
     * @param heapSize
     * @param fallbackNullFraction
     * @param extraQualifierMultiplier
     */
    public FakePartitionStatisticsImpl(String tableId, String partitionId, long numRows, long heapSize,double fallbackNullFraction, double extraQualifierMultiplier) {
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.numRows = numRows;
        this.heapSize = heapSize;
        this.fallbackNullFraction = fallbackNullFraction;
        this.extraQualifierMultiplier = extraQualifierMultiplier;
    }

    /**
     * Return row count.
     *
     * @return
     */
    @Override
    public long rowCount() {
        return numRows;
    }

    /**
     *
     * Return total size.
     *
     * @return
     */
    @Override
    public long totalSize() {
        return heapSize;
    }

    /**
     *
     * Return avg Row Width
     *
     * @return
     */
    @Override
    public int avgRowWidth() {
        return 100;
    }

    /**
     *
     * Partition ID
     *
     * @return
     */
    @Override
    public String partitionId() {
        return partitionId;
    }
    /**
     *
     * Get all Column Statistics as a list.
     *
     * @return
     */
    @Override
    public List<? extends ItemStatistics> getAllColumnStatistics() {
        return itemStatistics;
    }

    /**
     *
     * This is 1 based with the 0 entry being the key
     *
     * @param columnId the identifier of the column to fetch(indexed from 0)
     * @return
     */
    @Override
    public ItemStatistics getColumnStatistics(int columnId) {
        return itemStatistics.get(columnId);
    }
    /**
     *
     * Minvalue always null in fake partition
     *
     * @return
     */
    @Override
    public <T extends Comparator<T>> T minValue(int positionNumber) {
        return null;
    }
    /**
     *
     * Maxvalue always null in fake partition
     *
     * @return
     */
    @Override
    public <T extends Comparator<T>> T maxValue(int positionNumber) {
        return null;
    }
    /**
     *
     * Null Count = fallbackNullFraction * rowCount
     *
     *
     * @return
     */
    @Override
    public long nullCount(int positionNumber) {
        return (long) (fallbackNullFraction * (double) rowCount());
    }

    /**
     *
     * Not Null Count = (1.0 - fallbackNullFraction) * rowCount
     *
     * @param positionNumber
     * @return
     */
    @Override
    public long notNullCount(int positionNumber) {
        return (long) ( (1.0 - fallbackNullFraction) * (double) rowCount());
    }

    /**
     *
     * Cardinality = rowCount
     *
     * @param positionNumber
     * @return
     */
    @Override
    public long cardinality(int positionNumber) {
        return 0;
    }

    /**
     *
     * Selectivity = 0l, this is then handled by the caller to default to predefined default values.
     *
     * @param element the element to match
     * @param positionNumber
     * @param <T>
     * @return
     */
    @Override
    public <T extends Comparator<T>> long selectivity(T element, int positionNumber) {
        return 0l;
    }

    /**
     *
     * Selectivity = extraQualifierMultiplier * rowCount
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
        return (long) (extraQualifierMultiplier * (double) rowCount());
    }
}
