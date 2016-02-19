package com.splicemachine.derby.impl.stats;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionLoad;
import com.splicemachine.storage.StorageConfiguration;
import java.util.*;

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
            long partitionMaxFileSize=config.getLong(StorageConfiguration.REGION_MAX_FILE_SIZE);
            if(regionLoad==null){
                heapSize =partitionMaxFileSize;
            }else {
                heapSize = regionLoad.getStorefileSizeMB()+regionLoad.getMemStoreSizeMB();
                rowSizeRatio = ((double)heapSize)/partitionMaxFileSize;
            }
            long heapBytes = heapSize*1024*1024;
            long fbRegionRowCount = config.getLong(StatsConfiguration.FALLBACK_REGION_ROW_COUNT);
            long fbMinRowCount = config.getLong(StatsConfiguration.FALLBACK_MINIMUM_ROW_COUNT);
            long numRows = (long)(fbRegionRowCount*rowSizeRatio);
            if(numRows<fbMinRowCount)
                numRows = fbMinRowCount;
            if(heapBytes==0){
                heapBytes = numRows*config.getInt(StatsConfiguration.FALLBACK_ROW_WIDTH);
            }

            partitionStats.add(FakedPartitionStatistics.create(table,partition.getName(),
                    numRows,
                    heapBytes,
                    Collections.<ColumnStatistics>emptyList()));
        }
        return new GlobalStatistics(table,partitionStats);
    }
}
