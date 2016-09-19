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

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.access.configuration.StatsConfiguration;
import com.splicemachine.access.configuration.StorageConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionLoad;

/**
 * @author Scott Fines
 *         Date: 6/8/15
 */
public class RegionLoadStatistics{
    public static GlobalStatistics getParameterStatistics(String table, List<Partition> partitions) throws StandardException{
        SConfiguration config =EngineDriver.driver().getConfiguration();

        // Splits can cause us to think we do not have region load information for plan parsing, big problemo
        Map<String, PartitionLoad> regionIdToLoadMap = null;
        Collection<PartitionLoad> cachedRegionLoadsForTable = EngineDriver.driver().partitionLoadWatcher().tableLoad(table,false);
        while (true) {
            regionIdToLoadMap = new HashMap<>(cachedRegionLoadsForTable.size());
            for (PartitionLoad load : cachedRegionLoadsForTable)
                regionIdToLoadMap.put(load.getPartitionName(), load);
            if (partitions.size() == cachedRegionLoadsForTable.size())
                break;
            partitions.clear();
            PartitionStatsStore.getPartitions(table, partitions,true); // Refresh the partitions
            cachedRegionLoadsForTable = EngineDriver.driver().partitionLoadWatcher().tableLoad(table,true); // Refresh the region loads
        }

        List<OverheadManagedPartitionStatistics> partitionStats = new ArrayList<>(partitions.size());
        for(Partition partition:partitions){
            double rowSizeRatio = 1.0d;
            long heapSize;
            String partitionName = partition.getName();
            PartitionLoad regionLoad=regionIdToLoadMap.get(partitionName);
            long partitionMaxFileSize=config.getRegionMaxFileSize();
            if(regionLoad==null){
                heapSize =partitionMaxFileSize;
            }else {
                heapSize = ((long)(regionLoad.getStorefileSizeMB()+regionLoad.getMemStoreSizeMB()))*1024*1024;
                rowSizeRatio = ((double)heapSize)/partitionMaxFileSize;
            }
            long fbRegionRowCount = config.getFallbackRegionRowCount();
            long fbMinRowCount = config.getFallbackMinimumRowCount();
            long numRows = (long)(fbRegionRowCount*rowSizeRatio);
            if(numRows<fbMinRowCount)
                numRows = fbMinRowCount;
            if(heapSize==0){
                heapSize = numRows*config.getFallbackRowWidth();
            }

            partitionStats.add(FakedPartitionStatistics.create(table,partition.getName(),
                    numRows,
                    heapSize,
                    Collections.<ColumnStatistics>emptyList()));
        }
        return new GlobalStatistics(table,partitionStats);
    }

}
