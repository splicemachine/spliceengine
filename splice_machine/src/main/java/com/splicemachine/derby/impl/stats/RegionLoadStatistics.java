package com.splicemachine.derby.impl.stats;

import com.splicemachine.EngineDriver;
import com.splicemachine.access.api.SConfiguration;
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
    public static GlobalStatistics getParameterStatistics(String table, List<Partition> partitions){
        SConfiguration config =EngineDriver.driver().getConfiguration();

        Collection<PartitionLoad> cachedRegionLoadsForTable=EngineDriver.driver().partitionLoadWatcher().tableLoad(table);
        Map<String,PartitionLoad> regionIdToLoadMap = new HashMap<>(cachedRegionLoadsForTable.size());
        for(PartitionLoad load:cachedRegionLoadsForTable){
            String regionName=load.getPartitionName();
            regionName = regionName.substring(regionName.indexOf(".")+1,regionName.length()-1);
            regionIdToLoadMap.put(regionName,load);
        }

        List<OverheadManagedPartitionStatistics> partitionStats = new ArrayList<>(partitions.size());
        for(Partition partition:partitions){
            double rowSizeRatio = 1.0d;
            long heapSize;

            PartitionLoad regionLoad=regionIdToLoadMap.get(partition.getName());
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
