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

import com.splicemachine.db.iapi.sql.dictionary.ColumnStatisticsDescriptor;
import com.splicemachine.db.iapi.sql.dictionary.PartitionStatisticsDescriptor;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 *
 */
public class PartitionStatisticsImpl implements PartitionStatistics {
    PartitionStatisticsDescriptor partitionStatistics;
    private List<ItemStatistics> itemStatistics = new ArrayList<>();

    public PartitionStatisticsImpl() {

    }

    public PartitionStatisticsImpl(PartitionStatisticsDescriptor partitionStatistics) {
       this.partitionStatistics = partitionStatistics;
        for (ColumnStatisticsDescriptor columnStatisticsDescriptor : partitionStatistics.getColumnStatsDescriptors()) {
            itemStatistics.add(columnStatisticsDescriptor.getStats());
        }
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
        return itemStatistics.get(columnId);
    }

    @Override
    public <T extends Comparator<T>> T minValue(int positionNumber) {
        return (T) itemStatistics.get(positionNumber).minValue();
    }

    @Override
    public <T extends Comparator<T>> T maxValue(int positionNumber) {
        return (T) itemStatistics.get(positionNumber).maxValue();
    }

    @Override
    public long nullCount(int positionNumber) {
        return itemStatistics.get(positionNumber).nullCount();
    }

    @Override
    public long notNullCount(int positionNumber) {
        return itemStatistics.get(positionNumber).notNullCount();
    }

    @Override
    public long cardinality(int positionNumber) {
        return itemStatistics.get(positionNumber).cardinality();
    }

    @Override
    public <T extends Comparator<T>> long selectivity(T element, int positionNumber) {
        return itemStatistics.get(positionNumber).selectivity((T) element);
    }

    @Override
    public <T extends Comparator<T>> long rangeSelectivity(T start, T stop, boolean includeStart, boolean includeStop, int positionNumber) {
        return itemStatistics.get(positionNumber).rangeSelectivity((T) start, (T) stop, includeStart, includeStop);
    }
}
