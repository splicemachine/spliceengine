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

package com.splicemachine.db.iapi.sql.dictionary;

import java.util.List;

/**
 *
 * Descriptor representing table level statistics in the Splice Machine Database.
 *
 *
 */
public class TableStatisticsDescriptor extends TupleDescriptor {
    private long conglomerateId;
    private String partitionId;
    private long timestamp;
    private boolean stale;
    private int meanRowWidth;
    private long partitionSize;
    private long rowCount;
    private boolean inProgress;
    private List<ColumnStatisticsDescriptor> columnStatsDescriptors;

    public TableStatisticsDescriptor(long conglomerateId,
                                     String partitionId,
                                     long timestamp,
                                     boolean stale,
                                     boolean inProgress,
                                     long rowCount,
                                     long partitionSize,
                                     int meanRowWidth) {
        this.conglomerateId = conglomerateId;
        this.partitionId = partitionId;
        this.timestamp = timestamp;
        this.stale = stale;
        this.meanRowWidth = meanRowWidth;
        this.partitionSize = partitionSize;
        this.rowCount = rowCount;
        this.inProgress = inProgress;
    }

    public long getConglomerateId() { return conglomerateId; }
    public String getPartitionId() { return partitionId; }
    public long getTimestamp() { return timestamp; }
    public boolean isStale() { return stale; }
    public int getMeanRowWidth() { return meanRowWidth; }
    public long getPartitionSize() { return partitionSize; }
    public long getRowCount() { return rowCount; }
    public boolean isInProgress() { return inProgress; }

    public List<ColumnStatisticsDescriptor> getColumnStatsDescriptors() {
        return columnStatsDescriptors;
    }

    public void setColumnStatsDescriptors(List<ColumnStatisticsDescriptor> columnStatsDescriptors) {
        this.columnStatsDescriptors = columnStatsDescriptors;
    }

}