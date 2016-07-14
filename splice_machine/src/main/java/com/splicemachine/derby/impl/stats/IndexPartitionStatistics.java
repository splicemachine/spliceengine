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

package com.splicemachine.derby.impl.stats;

import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.PartitionStatistics;
import com.splicemachine.stats.estimate.Distribution;

import java.util.List;

/**
 * @author Scott Fines
 *         Date: 5/5/15
 */
public class IndexPartitionStatistics implements OverheadManagedPartitionStatistics{
    private final OverheadManagedPartitionStatistics indexPartitionStats;
    private final OverheadManagedTableStatistics baseTableStatistics;

    private final double indexSizeFactor;

    public IndexPartitionStatistics(OverheadManagedPartitionStatistics indexPartitionStats,OverheadManagedTableStatistics baseTableStatistics){
        this.indexPartitionStats=indexPartitionStats;
        this.baseTableStatistics=baseTableStatistics;
        this.indexSizeFactor = Math.abs(((double)indexPartitionStats.avgRowWidth())/baseTableStatistics.avgRowWidth());
    }

    @Override public double getOpenScannerLatency(){ return baseTableStatistics.openScannerLatency(); }
    @Override public double getCloseScannerLatency(){ return baseTableStatistics.openScannerLatency(); }

    @Override public long numOpenEvents(){ return baseTableStatistics.numOpenEvents(); }
    @Override public long numCloseEvents(){ return baseTableStatistics.numCloseEvents(); }
    @Override public long rowCount(){ return indexPartitionStats.rowCount(); }
    @Override public long totalSize(){ return indexPartitionStats.totalSize(); }
    @Override public int avgRowWidth(){ return indexPartitionStats.avgRowWidth(); }
    @Override public long queryCount(){ return indexPartitionStats.queryCount(); }
    @Override public String partitionId(){ return indexPartitionStats.partitionId(); }
    @Override public String tableId(){ return indexPartitionStats.tableId(); }
    @Override public double localReadLatency(){ return indexPartitionStats.localReadLatency(); }
    @Override public long localReadTime(){ return indexPartitionStats.localReadTime(); }
    @Override public long collectionTime(){ return indexPartitionStats.collectionTime(); }

    @Override
    public double remoteReadLatency(){
        return indexSizeFactor*baseTableStatistics.remoteReadLatency();
    }

    @Override
    public long remoteReadTime(){
        return (long)(indexSizeFactor*baseTableStatistics.remoteReadTime());
    }

    @Override public List<ColumnStatistics> columnStatistics(){ return indexPartitionStats.columnStatistics(); }
    @Override public <T> ColumnStatistics<T> columnStatistics(int columnId){ return indexPartitionStats.columnStatistics(columnId); }
    @Override public <T extends Comparable<T>> Distribution<T> columnDistribution(int columnId){ return indexPartitionStats.columnDistribution(columnId); }

    @Override
    public PartitionStatistics merge(PartitionStatistics other){
        indexPartitionStats.merge(other);
        return this;
    }

    public double multiGetLatency(){
        return indexPartitionStats.remoteReadLatency();
    }
}
