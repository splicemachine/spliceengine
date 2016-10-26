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
 *
 *
 *
 */
public class TableStatisticsImpl implements TableStatistics {
    private String tableId;
    private List<? extends PartitionStatistics> partitionStatistics;
    private PartitionStatistics effectivePartitionStatistics;
    private long rowCount = 0l;
    private long totalSize = 0;
    private int avgRowWidth = 0;


    public TableStatisticsImpl() {

    }

    public TableStatisticsImpl(String tableId,
                               List<? extends PartitionStatistics> partitionStatistics) {
        this.tableId = tableId;
        this.partitionStatistics = partitionStatistics;
    }

    @Override
    public String tableId() {
        return tableId;
    }

    @Override
    public long rowCount() {
        return getEffectivePartitionStatistics().rowCount();
    }

    @Override
    public int avgRowWidth() {
        return getEffectivePartitionStatistics().avgRowWidth();
    }

    @Override
    public List<? extends PartitionStatistics> getPartitionStatistics() {
        return partitionStatistics;
    }

    @Override
    public PartitionStatistics getEffectivePartitionStatistics()  {
        try {
            ColumnStatisticsMerge[] itemStatisticsBuilder = null;
            boolean fake = false;
            if (effectivePartitionStatistics == null) {
                for (PartitionStatistics partStats : partitionStatistics) {
                    List<? extends ItemStatistics> itemStatisticsList = partStats.getAllColumnStatistics();
                    rowCount += partStats.rowCount();
                    totalSize += partStats.totalSize();
                    avgRowWidth += partStats.avgRowWidth(); // todo fix
                    if (itemStatisticsList.size() ==0)
                        fake = true;
                    if (itemStatisticsBuilder == null)
                        itemStatisticsBuilder = new ColumnStatisticsMerge[itemStatisticsList.size()];
                    for (int i = 0; i < itemStatisticsList.size(); i++) {
                        if (itemStatisticsBuilder[i] == null)
                            itemStatisticsBuilder[i] = ColumnStatisticsMerge.instance();
                        itemStatisticsBuilder[i].accumulate((ColumnStatisticsImpl)itemStatisticsList.get(i));
                    }
                }
                    if (fake)
                        effectivePartitionStatistics = new FakePartitionStatisticsImpl(tableId,null,rowCount,totalSize);
                    else {
                        effectivePartitionStatistics = new EffectivePartitionStatisticsImpl(itemStatisticsBuilder,
                                rowCount, totalSize,
                                avgRowWidth);
                    }
            }
            return effectivePartitionStatistics;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T extends Comparator<T>> T minValue(int positionNumber) {
        return getEffectivePartitionStatistics().minValue(positionNumber);
    }

    @Override
    public <T extends Comparator<T>> T maxValue(int positionNumber) {
        return getEffectivePartitionStatistics().maxValue(positionNumber);
    }

    @Override
    public long nullCount(int positionNumber) {
        return getEffectivePartitionStatistics().nullCount(positionNumber);
    }

    @Override
    public long notNullCount(int positionNumber) {
        return getEffectivePartitionStatistics().notNullCount(positionNumber);
    }

    @Override
    public long cardinality(int positionNumber) {
        long cardinality = getEffectivePartitionStatistics().cardinality(positionNumber);
        return cardinality >= rowCount()?
                rowCount():
                getEffectivePartitionStatistics().cardinality(positionNumber);
    }

    @Override
    public <T extends Comparator<T>> double selectivity(T element, int positionNumber) {
        return getEffectivePartitionStatistics().selectivity(element,positionNumber)/getEffectivePartitionStatistics().rowCount();
    }
    // Do we want to do do effectivePartitionStatistics for performance?
    @Override
    public <T extends Comparator<T>> double rangeSelectivity(T start, T stop, boolean includeStart, boolean includeStop, int positionNumber) {
        long selectivity = 0l;
        long rowCount = 0l;
        for (PartitionStatistics partitionStatistic :partitionStatistics) {
            selectivity += partitionStatistic.rangeSelectivity(start,stop,includeStart,includeStop,positionNumber);
            rowCount += partitionStatistic.rowCount();
        }
        return ((double)selectivity/(double)rowCount);
    }

    @Override
    public int numPartitions() {
        return partitionStatistics.size();
    }


}
