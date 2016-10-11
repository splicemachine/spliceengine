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

import com.splicemachine.db.iapi.sql.dictionary.PartitionStatisticsDescriptor;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 *
 */
public class FakePartitionStatisticsImpl implements PartitionStatistics {
//    PartitionStatisticsDescriptor partitionStatistics;
    private List<? extends ItemStatistics> itemStatistics = Collections.EMPTY_LIST;
    String tableId;
    String partitionId;
    long numRows;
    long heapSize;


    public FakePartitionStatisticsImpl(String tableId, String partitionId, long numRows, long heapSize) {
        this.tableId = tableId;
        this.partitionId = partitionId;
        this.numRows = numRows;
        this.heapSize = heapSize;
    }

    @Override
    public long rowCount() {
        return numRows;
    }

    @Override
    public long totalSize() {
        return heapSize;
    }

    @Override
    public int avgRowWidth() {
        return 100;
    }

    @Override
    public String partitionId() {
        return partitionId;
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
        return null;
    }

    @Override
    public <T extends Comparator<T>> T maxValue(int positionNumber) {
        return null;
    }

    @Override
    public long nullCount(int positionNumber) {
        return 0;
    }

    @Override
    public long notNullCount(int positionNumber) {
        return rowCount();
    }

    @Override
    public long cardinality(int positionNumber) {
        return rowCount();
    }

    @Override
    public <T extends Comparator<T>> long selectivity(T element, int positionNumber) {
        return 0;
    }

    @Override
    public <T extends Comparator<T>> long rangeSelectivity(T start, T stop, boolean includeStart, boolean includeStop, int positionNumber) {
        return 0;
    }
}
