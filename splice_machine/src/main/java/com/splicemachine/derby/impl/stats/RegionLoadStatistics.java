/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.stats;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.stats.FakePartitionStatisticsImpl;
import com.splicemachine.db.iapi.stats.PartitionStatistics;
import com.splicemachine.db.iapi.stats.TableStatistics;
import com.splicemachine.db.iapi.stats.TableStatisticsImpl;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionLoad;

/**
 * @author Scott Fines
 *         Date: 6/8/15
 */
public class RegionLoadStatistics{
    public static TableStatistics getTableStatistics(String table, List<Partition> partitions, double fallbackNullFraction, double extraQualifierMultiplier) throws StandardException{
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
            StoreCostControllerImpl.getPartitions(table, partitions,true); // Refresh the partitions
            cachedRegionLoadsForTable = EngineDriver.driver().partitionLoadWatcher().tableLoad(table,true); // Refresh the region loads
        }

        List<PartitionStatistics> partitionStats = new ArrayList<>(partitions.size());
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

            partitionStats.add(new FakePartitionStatisticsImpl(table,partition.getName(),
                    numRows,
                    heapSize,fallbackNullFraction,extraQualifierMultiplier));
        }
        return new TableStatisticsImpl(table,partitionStats,fallbackNullFraction,extraQualifierMultiplier);
    }

}
