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
