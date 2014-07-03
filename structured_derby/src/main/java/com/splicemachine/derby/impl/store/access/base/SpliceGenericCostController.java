package com.splicemachine.derby.impl.store.access.base;

import java.util.Arrays;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

import org.apache.derby.impl.store.access.conglomerate.GenericCostController;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.RegionLoad;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.splicemachine.hbase.HBaseRegionCache;
import com.splicemachine.hbase.HBaseRegionLoads;
import com.splicemachine.utils.SpliceLogUtils;

public abstract class SpliceGenericCostController extends GenericCostController {
    private static final Logger LOG = Logger.getLogger(SpliceGenericCostController.class);

    protected static long computeRowCount(SortedSet<HRegionInfo> regions, Map<String,RegionLoad> regionLoads, long constantRowSize, long hfileMaxSize, Scan scan) {
		long rowCount = 0;
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "computeRowCount {regions={%s}, regionLoad={%s}, constantRowSize=%d, hfileMaxSize=%d, scan={%s}",
					regions==null?"null":Arrays.toString(regions.toArray()), regionLoads==null?"null":Arrays.toString(regionLoads.keySet().toArray()), constantRowSize, hfileMaxSize, scan);		
		for (HRegionInfo info: regions) {
			if (isRegionInScan(scan,info)) {
				if (LOG.isTraceEnabled())
					SpliceLogUtils.trace(LOG, "regionInfo with encodedname {%s} and region name as string %s", info.getEncodedName(), info.getRegionNameAsString());				
				rowCount+=getRowSize(constantRowSize,regionLoads==null?null:regionLoads.get(info.getRegionNameAsString()),hfileMaxSize);	
			}
		}
		return rowCount;
	}

	protected static boolean isRegionInScan(Scan scan, HRegionInfo regionInfo) {
		return true;		
	}
	
	protected static long getRowSize(long constantRowSize, RegionLoad regionLoad, long hfileMaxSize) {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "getRowSize with constantRowSize %d and regionLoad %s and hfileMaxSize %d",constantRowSize, regionLoad, hfileMaxSize);
		if (regionLoad==null)
			return constantRowSize; // No Metrics
		long rowSize = constantRowSize*(HBaseRegionLoads.memstoreAndStorefileSize(regionLoad)/hfileMaxSize);
		return rowSize < 20?20l:rowSize;
	}
	
    protected static SortedSet<HRegionInfo> getRegions(long conglomId) {
        String table = Long.toString(conglomId);
        try {
            return HBaseRegionCache.getInstance().getRegions(Bytes.toBytes(table));
        } catch (ExecutionException e) {
        	SpliceLogUtils.error(LOG, "Erorr in getRegions on the cost controller, should not happen", e);
        	return null;
        }
    }
	
}
