package com.splicemachine.derby.utils;

import com.splicemachine.hbase.regioninfocache.HBaseRegionCache;
import com.splicemachine.hbase.regioninfocache.RegionCache;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.stats.ColumnStatistics;
import com.splicemachine.stats.frequency.FrequentElements;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.util.Pair;

import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 3/2/15
 */
public class StatisticsFunctions {

    public static long CARDINALITY(ColumnStatistics columnStatistics){
        if(columnStatistics==null) return 0;
        return columnStatistics.cardinality();
    }

    public static long NULL_COUNT(ColumnStatistics columnStatistics){
        if(columnStatistics==null) return 0;
        return columnStatistics.nullCount();
    }

    public static float NULL_FRACTION(ColumnStatistics columnStatistics){
        if(columnStatistics==null) return 0;
        return columnStatistics.nullFraction();
    }

    public static String TOP_K(ColumnStatistics columnStatistics){
        if(columnStatistics==null) return null;
        FrequentElements frequentElements = columnStatistics.topK();
        StringBuilder string = new StringBuilder();
        boolean isFirst = true;
        for(Object estimate:frequentElements.allFrequentElements()){
            if(!isFirst) string = string.append(",");
            else isFirst = false;
            string = string.append(estimate);
        }
        return string.toString();
    }

    public static boolean PARTITION_EXISTS(long conglomerateId,String partitionId) throws StandardException {
        RegionCache regionCache = HBaseRegionCache.getInstance();
        try {
            SortedSet<Pair<HRegionInfo, ServerName>> regions = regionCache.getRegions(Long.toString(conglomerateId).getBytes());
            for(Pair<HRegionInfo,ServerName> region:regions){
                if(region.getFirst().getEncodedName().equals(partitionId)) return true;
            }
            return false;
        } catch (ExecutionException e) {
            throw Exceptions.parseException(e.getCause());
        }
    }

}
