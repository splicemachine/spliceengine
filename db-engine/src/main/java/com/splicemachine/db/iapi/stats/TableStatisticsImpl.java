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
 * Implementation of Table Level Statistics.  The partition statistics contained within have each partition we know about.
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
    private double fallbackNullFraction;
    private double extraQualifierMultiplier;

    public TableStatisticsImpl() {

    }

    /**
     *
     * Create Table Statistics
     *
     * @param tableId
     * @param partitionStatistics
     * @param fallbackNullFraction
     * @param extraQualifierMultiplier
     */
    public TableStatisticsImpl(String tableId,
                               List<? extends PartitionStatistics> partitionStatistics,
                               double fallbackNullFraction,
                               double extraQualifierMultiplier) {
        this.tableId = tableId;
        this.partitionStatistics = partitionStatistics;
        assert partitionStatistics.size() > 0:"Partition statistics are 0";
        this.fallbackNullFraction = fallbackNullFraction;
        this.extraQualifierMultiplier = extraQualifierMultiplier;
    }

    /**
     * Table ID
     *
     * @return
     */
    @Override
    public String tableId() {
        return tableId;
    }

    /**
     *
     * Row Count from the effective partition statistics
     *
     * @return
     */
    @Override
    public long rowCount() {
        return getEffectivePartitionStatistics().rowCount();
    }

    /**
     *
     * Average Row Width from the effective partition statistics
     *
     * @return
     */
    @Override
    public int avgRowWidth() {
        return getEffectivePartitionStatistics().avgRowWidth();
    }

    /**
     *
     * Retrieve the partition statistics
     *
     * @return
     */
    @Override
    public List<? extends PartitionStatistics> getPartitionStatistics() {
        return partitionStatistics;
    }

    /**
     *
     * Retrieve the effective partition statistics (i.e. the stats for the whole table)
     *
     * @return
     */
    @Override
    public PartitionStatistics getEffectivePartitionStatistics()  {
        try {
            ColumnStatisticsMerge[] itemStatisticsBuilder = null;
            boolean fake = false;
            if (effectivePartitionStatistics == null) {
                assert partitionStatistics !=null:"Partition Statistics are null";
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
                    if (fake) {
                        effectivePartitionStatistics = new FakePartitionStatisticsImpl(tableId, null, rowCount, totalSize,
                                fallbackNullFraction,extraQualifierMultiplier);
                    }
                    else {
                        effectivePartitionStatistics = new EffectivePartitionStatisticsImpl(itemStatisticsBuilder,
                                rowCount, totalSize,
                                avgRowWidth,fallbackNullFraction,extraQualifierMultiplier);
                    }
            }
            return effectivePartitionStatistics;
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    /**
     *
     * Retrieve the minimum typed value
     *
     * @param positionNumber
     * @param <T>
     * @return
     */
    @Override
    public <T extends Comparator<T>> T minValue(int positionNumber) {
        return getEffectivePartitionStatistics().minValue(positionNumber);
    }

    /**
     *
     * Retrieve the maximum typed value
     *
     * @param positionNumber
     * @param <T>
     * @return
     */
    @Override
    public <T extends Comparator<T>> T maxValue(int positionNumber) {
        return getEffectivePartitionStatistics().maxValue(positionNumber);
    }

    /**
     *
     * Retrieve the null count (0 based) from the effective partition statistics
     *
     * @param positionNumber
     * @return
     */
    @Override
    public long nullCount(int positionNumber) {
        return getEffectivePartitionStatistics().nullCount(positionNumber);
    }
    /**
     *
     * Retrieve the not null count (0 based) from the effective partition statistics
     *
     * @param positionNumber
     * @return
     */
    @Override
    public long notNullCount(int positionNumber) {
        return getEffectivePartitionStatistics().notNullCount(positionNumber);
    }
    /**
     *
     * Retrieve the cardinality (0 based) from the effective partition statistics.
     *
     * @param positionNumber
     * @return
     */
    @Override
    public long cardinality(int positionNumber) {
        long cardinality = getEffectivePartitionStatistics().cardinality(positionNumber);
        return cardinality >= rowCount()?
                rowCount():
                getEffectivePartitionStatistics().cardinality(positionNumber);
    }

    /**
     *
     * Return number of rows that are selected.
     *
     * @param element the element to match
     * @param positionNumber
     * @param <T>
     * @return
     */
    @Override
    public <T extends Comparator<T>> double selectivity(T element, int positionNumber) {
        return getEffectivePartitionStatistics().selectivity(element,positionNumber)/getEffectivePartitionStatistics().rowCount();
    }

    /**
     *
     *      Range selectivity performed on each partition...
     *
     *     Do we want to do do effectivePartitionStatistics for performance?
     */
    @Override
    public <T extends Comparator<T>> double rangeSelectivity(T start, T stop, boolean includeStart, boolean includeStop, int positionNumber) {
        long selectivity = 0l;
        long rowCount = 0l;
        for (PartitionStatistics partitionStatistic :partitionStatistics) {
            selectivity += partitionStatistic.rangeSelectivity(start,stop,includeStart,includeStop,positionNumber);
            rowCount += partitionStatistic.rowCount();
        }
        if (rowCount == 0)
            return 0;
        return ((double)selectivity/(double)rowCount);
    }

    /**
     *
     * Return the number of partitions.
     *
     * @return
     */
    @Override
    public int numPartitions() {
        return partitionStatistics.size();
    }


}
