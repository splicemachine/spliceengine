/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2016 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.iapi.stats;

import com.splicemachine.db.iapi.error.StandardException;
import java.util.Comparator;
import java.util.List;

/**
 *
 */
public class EffectivePartitionStatisticsImpl implements PartitionStatistics {
    private ItemStatistics[] itemStatistics;
    private long rowCount;
    private long totalSize;
    private int avgRowWidth;

    public EffectivePartitionStatisticsImpl() {

    }

    public EffectivePartitionStatisticsImpl(ItemStatisticsBuilder[] itemStatisticsBuilder,
                                            long rowCount, long totalSize,
                                            int avgRowWidth) {
        this.rowCount = rowCount;
        this.totalSize = totalSize;
        this.avgRowWidth = avgRowWidth;
       itemStatistics = new ItemStatistics[itemStatisticsBuilder.length];
       for (int i =0; i<itemStatisticsBuilder.length;i++) {
            itemStatistics[i] = new ColumnStatisticsImpl( null,
                    itemStatisticsBuilder[i].buildQuantilesSketch(),
                    itemStatisticsBuilder[i].frequenciesSketch,
                    itemStatisticsBuilder[i].buildThetaSketch(),
                    itemStatisticsBuilder[i].nullCount
                    );

       }
    }

    @Override
    public long rowCount() {
        return rowCount;
    }

    @Override
    public long totalSize() {
        return totalSize;
    }

    @Override
    public int avgRowWidth() {
        return avgRowWidth;
    }

    @Override
    public String partitionId() {
        return null;
    }

    @Override
    public List<? extends ItemStatistics> getAllColumnStatistics() {
        throw new UnsupportedOperationException("Use getAllColumnStatistics on the table vs. agains the effective partition.");
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
        return itemStatistics[columnId];
    }
/*
    @Override
    public void mergeItemStatistics(ItemStatisticsBuilder[] itemStatisticsBuilders) throws StandardException {
        for (int i = 0; i<itemStatistics.size();i++) {
            itemStatisticsBuilders[i] = itemStatistics.get(i).mergeInto(itemStatisticsBuilders[i]);
        }
    }
*/
    @Override
    public <T extends Comparator<T>> T minValue(int positionNumber) {
        return (T) itemStatistics[positionNumber].minValue();
    }

    @Override
    public <T extends Comparator<T>> T maxValue(int positionNumber) {
        return (T) itemStatistics[positionNumber].maxValue();
    }

    @Override
    public long nullCount(int positionNumber) {
        return itemStatistics[positionNumber].nullCount();
    }

    @Override
    public long notNullCount(int positionNumber) {
        return itemStatistics[positionNumber].notNullCount();
    }

    @Override
    public long cardinality(int positionNumber) {
        return itemStatistics[positionNumber].cardinality();
    }

    @Override
    public <T extends Comparator<T>> long selectivity(T element, int positionNumber) {
        return itemStatistics[positionNumber].selectivity((T) element);
    }

    @Override
    public <T extends Comparator<T>> long rangeSelectivity(T start, T stop, boolean includeStart, boolean includeStop, int positionNumber) {
        throw new UnsupportedOperationException("Use Range Selectivity on the table vs. agains the effective partition.");
    }
}
