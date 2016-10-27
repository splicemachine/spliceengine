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

package com.splicemachine.db.iapi.sql.dictionary;

import com.splicemachine.db.iapi.stats.ItemStatistics;

/**
 *
 * Column Statistics Descriptor for representing column
 * statistical data in the Splice Machine data dictionary.
 *
 * @author Scott Fines
 *         Date: 2/25/15
 */
public class ColumnStatisticsDescriptor extends TupleDescriptor {
    private long conglomerateId;
    private String partitionId;
    private int columnId;
    private ItemStatistics itemStatistics;

    public ColumnStatisticsDescriptor(long conglomerateId,
                                      String partitionId,
                                      int columnId,
                                      ItemStatistics itemStatistics) {
        this.conglomerateId = conglomerateId;
        this.partitionId = partitionId;
        this.columnId = columnId;
        this.itemStatistics = itemStatistics;
    }

    /**
     * Return column ID
     *
     * @return
     */
    public int getColumnId() { return columnId; }

    /**
     * Return conglomerate id
     *
     * @return
     */
    public long getConglomerateId() { return conglomerateId; }

    /**
     * Return partition id
     *
     * @return
     */
    public String getPartitionId() { return partitionId; }

    /**
     *
     * Retrieve the statistics
     *
     * @return
     */
    public ItemStatistics getStats() { return itemStatistics; }
}
