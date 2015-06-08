package com.splicemachine.derby.impl.stats;

import com.splicemachine.hbase.HBaseRegionLoads;
import com.splicemachine.stats.ColumnStatistics;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;

import java.util.*;

/**
 * @author Scott Fines
 *         Date: 6/8/15
 */
public class RegionLoadStatistics{
    public static GlobalStatistics getParameterStatistics(String table, List<HRegionInfo> partitions){
        long perRowLocalLatency = 1;
        long perRowRemoteLatency = StatsConstants.fallbackRemoteLatencyRatio*perRowLocalLatency;


        Collection<RegionLoad> cachedRegionLoadsForTable=HBaseRegionLoads.getCachedRegionLoadsForTable(table);
        Map<String,RegionLoad> regionIdToLoadMap = new HashMap<>(cachedRegionLoadsForTable.size());
        for(RegionLoad load:cachedRegionLoadsForTable){
            String regionName=load.getNameAsString();
            regionName = regionName.substring(regionName.indexOf(".")+1,regionName.length()-1);
            regionIdToLoadMap.put(regionName,load);
        }

        List<OverheadManagedPartitionStatistics> partitionStats = new ArrayList<>(partitions.size());
        for(HRegionInfo partition:partitions){
            double rowSizeRatio = 1.0d;
            long heapSize;

            RegionLoad regionLoad=regionIdToLoadMap.get(partition.getEncodedName());
            if(regionLoad==null){
                heapSize = StatsConstants.regionMaxFileSize;
            }else {
                heapSize = regionLoad.getStorefileSizeMB()+regionLoad.getMemStoreSizeMB();
                rowSizeRatio = ((double)heapSize)/StatsConstants.regionMaxFileSize;
            }
            long heapBytes = heapSize*1024*1024;
            long numRows = (long)(StatsConstants.fallbackRegionRowCount*rowSizeRatio);
            if(numRows<StatsConstants.fallbackMinimumRowCount)
                numRows = StatsConstants.fallbackMinimumRowCount;
            if(heapBytes==0){
                heapBytes = numRows*StatsConstants.fallbackRowWidth;
            }

            partitionStats.add(new FakedPartitionStatistics(table,partition.getEncodedName(),
                    numRows,
                    heapBytes,
                    0l,
                    perRowLocalLatency*numRows,
                    perRowRemoteLatency*numRows,
                    perRowRemoteLatency,1l,
                    perRowRemoteLatency,1l,
                    Collections.<ColumnStatistics>emptyList()));
        }
        return new GlobalStatistics(table,partitionStats);
    }
}
