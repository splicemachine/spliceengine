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

import com.splicemachine.db.iapi.sql.dictionary.ColumnStatisticsDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.PartitionStatisticsDescriptor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 *
 */
public class PartitionStatisticsImpl implements PartitionStatistics {
    PartitionStatisticsDescriptor partitionStatistics;
    private List<ItemStatistics> itemStatistics = new ArrayList<>();
    double fallbackNullFraction;
    double extraQualifierMultiplier;

    public PartitionStatisticsImpl() {

    }

    public PartitionStatisticsImpl(PartitionStatisticsDescriptor partitionStatistics,
                                   double fallbackNullFraction,
                                   double extraQualifierMultiplier) {
        this.partitionStatistics = partitionStatistics;

        // find the max column with stats collected
        int maxColId = 0;
        for (ColumnStatisticsDescriptor columnStatisticsDescriptor : partitionStatistics.getColumnStatsDescriptors()) {
            if (columnStatisticsDescriptor.getColumnId() > maxColId)
                maxColId = columnStatisticsDescriptor.getColumnId();
        }

        ItemStatistics[] tmpArray = new ItemStatistics[maxColId];
        for (ColumnStatisticsDescriptor columnStatisticsDescriptor : partitionStatistics.getColumnStatsDescriptors()) {
            tmpArray[columnStatisticsDescriptor.getColumnId()-1] = columnStatisticsDescriptor.getStats();
        }
        itemStatistics = Arrays.asList(tmpArray);
        this.fallbackNullFraction = fallbackNullFraction;
        this.extraQualifierMultiplier = extraQualifierMultiplier;
    }

    @Override
    public long rowCount() {
        return partitionStatistics.getRowCount();
    }

    @Override
    public long totalSize() {
        return (long) partitionStatistics.getMeanRowWidth()*partitionStatistics.getRowCount();
    }

    @Override
    public int avgRowWidth() {
        return partitionStatistics.getMeanRowWidth();
    }

    @Override
    public String partitionId() {
        return partitionStatistics.getPartitionId();
    }

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
        return columnId >= itemStatistics.size()?null:itemStatistics.get(columnId);
    }

    @Override
    public PartitionStatisticsDescriptor getPartitionStatistics() {
        return partitionStatistics;
    }

    @Override
    public <T extends Comparator<T>> T minValue(int positionNumber) {
        ItemStatistics stats = positionNumber >= itemStatistics.size()?null:itemStatistics.get(positionNumber);
        return (T) (stats == null? null:stats.minValue());
    }

    @Override
    public <T extends Comparator<T>> T maxValue(int positionNumber) {
        ItemStatistics stats = positionNumber >= itemStatistics.size()?null:itemStatistics.get(positionNumber);
        return (T) (stats == null? null:stats.maxValue());
    }

    @Override
    public long nullCount(int positionNumber) {
        ItemStatistics stats = positionNumber >= itemStatistics.size()?null:itemStatistics.get(positionNumber);
        if (stats==null)
            return (long) (fallbackNullFraction * (double)rowCount());
        else
            return stats.nullCount();
    }

    @Override
    public long notNullCount(int positionNumber) {
        ItemStatistics stats = positionNumber >= itemStatistics.size()?null:itemStatistics.get(positionNumber);
        if (stats == null)
            return (long) ((1.0 - fallbackNullFraction) * (double)rowCount());
        else
            return stats.notNullCount();
    }

    @Override
    public long cardinality(int positionNumber) {
        ItemStatistics stats = positionNumber >= itemStatistics.size()?null:itemStatistics.get(positionNumber);
        return stats==null?rowCount():stats.cardinality();
    }

    @Override
    public <T extends Comparator<T>> long selectivity(T element, int positionNumber) {
        ItemStatistics stats = positionNumber >= itemStatistics.size()?null:itemStatistics.get(positionNumber);
        return stats==null?(long) (( (double) rowCount()) * extraQualifierMultiplier ):stats.selectivity((T) element);
    }

    @Override
    public <T extends Comparator<T>> long rangeSelectivity(T start, T stop, boolean includeStart, boolean includeStop, int positionNumber, boolean useExtrapolation) {
        ItemStatistics stats = positionNumber >= itemStatistics.size()?null:itemStatistics.get(positionNumber);
        if (stats == null)
            return (long) (extraQualifierMultiplier * (double) rowCount());
        else
            return stats.rangeSelectivity((T) start, (T) stop, includeStart, includeStop, useExtrapolation);
    }

    @Override
    public <T extends Comparator<T>> long selectivityExcludingValueIfSkewed(T element, int positionNumber) {
        ItemStatistics stats = positionNumber >= itemStatistics.size()?null:itemStatistics.get(positionNumber);
        return stats==null?(long) (( (double) rowCount()) * extraQualifierMultiplier ):stats.selectivityExcludingValueIfSkewed((T) element);
    }
}
