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

import com.splicemachine.db.iapi.sql.dictionary.ColumnStatisticsDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.PartitionStatisticsDescriptor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

/**
 *
 */
public class PartitionStatisticsImpl implements PartitionStatistics {
    PartitionStatisticsDescriptor partitionStatistics;
    private List<ItemStatistics> itemStatistics = new ArrayList<>();
    double fallbackNullFraction;
    double extraQualifierMultiplier;

    /* map between columnId and index into the itemStatistics list */
    private HashMap<Integer,Integer> colIndex = new HashMap<>();

    public PartitionStatisticsImpl() {

    }

    public PartitionStatisticsImpl(PartitionStatisticsDescriptor partitionStatistics,
                                   double fallbackNullFraction,
                                   double extraQualifierMultiplier) {
       this.partitionStatistics = partitionStatistics;
        for (ColumnStatisticsDescriptor columnStatisticsDescriptor : partitionStatistics.getColumnStatsDescriptors()) {
            itemStatistics.add(columnStatisticsDescriptor.getStats());
            colIndex.put(columnStatisticsDescriptor.getColumnId()-1, itemStatistics.size()-1);
        }
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
        Integer index = colIndex.get(columnId);
        return index==null?null:itemStatistics.get(index);
    }

    @Override
    public <T extends Comparator<T>> T minValue(int positionNumber) {
        Integer index = colIndex.get(positionNumber);
        /* if no stats available, follow the same behavior as FakePartitionStatisticsImpl */
        return (T) (index==null?null:itemStatistics.get(index).minValue());
    }

    @Override
    public <T extends Comparator<T>> T maxValue(int positionNumber) {
        Integer index = colIndex.get(positionNumber);
        /* if no stats available, follow the same behavior as FakePartitionStatisticsImpl */
        return (T)(index==null?null:itemStatistics.get(index).maxValue());
    }

    @Override
    public long nullCount(int positionNumber) {
        Integer index = colIndex.get(positionNumber);
        /* if no stats available, follow the same behavior as FakePartitionStatisticsImpl */
        if (index==null)
            return (long) (fallbackNullFraction * (double)rowCount());
        else
            return itemStatistics.get(index).nullCount();
    }

    @Override
    public long notNullCount(int positionNumber) {
        Integer index = colIndex.get(positionNumber);
        /* if no stats available, follow the same behavior as FakePartitionStatisticsImpl */
        if (index == null)
            return (long) ((1.0 - fallbackNullFraction) * (double)rowCount());
        else
            return itemStatistics.get(index).notNullCount();
    }

    @Override
    public long cardinality(int positionNumber) {
        Integer index = colIndex.get(positionNumber);
        /* if no stats available, follow the same behavior as FakePartitionStatisticsImpl */
        if (index == null)
            return 0;
        else
            return itemStatistics.get(index).cardinality();
    }

    @Override
    public <T extends Comparator<T>> long selectivity(T element, int positionNumber) {
        Integer index = colIndex.get(positionNumber);
        /* if no stats available, follow the same behavior as FakePartitionStatisticsImpl */
        if (index == null)
            return 0L;
        else
            return itemStatistics.get(index).selectivity((T) element);
    }

    @Override
    public <T extends Comparator<T>> long rangeSelectivity(T start, T stop, boolean includeStart, boolean includeStop, int positionNumber) {
        Integer index = colIndex.get(positionNumber);
        /* if no stats available, follow the same behavior as FakePartitionStatisticsImpl */
        if (index == null)
            return (long) (extraQualifierMultiplier * (double) rowCount());
        else
            return itemStatistics.get(index).rangeSelectivity((T) start, (T) stop, includeStart, includeStop);
    }

    @Override
    public HashMap<Integer, Integer> getColIndex() {
        return colIndex;
    }
}
