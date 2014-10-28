package com.splicemachine.derby.impl.store.access.base;

import java.util.Arrays;
import java.util.Map;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.impl.store.access.conglomerate.GenericCostController;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.HServerLoad.RegionLoad;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.hbase.HBaseRegionCache;
import com.splicemachine.hbase.HBaseRegionLoads;
import com.splicemachine.utils.SpliceLogUtils;

public abstract class SpliceGenericCostController extends GenericCostController {
    private static final Logger LOG = Logger.getLogger(SpliceGenericCostController.class);

    protected static long computeRowCount(SortedSet<Pair<HRegionInfo,ServerName>> regions, Map<String,RegionLoad> regionLoads, long constantRowSize, long hfileMaxSize, Scan scan) {
		long rowCount = 0;
		int numberOfRegionsInvolved = 0;
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "computeRowCount {regions={%s}, regionLoad={%s}, constantRowSize=%d, hfileMaxSize=%d, scan={%s}",
					regions==null?"null":Arrays.toString(regions.toArray()), regionLoads==null?"null":Arrays.toString(regionLoads.keySet().toArray()), constantRowSize, hfileMaxSize, scan);		
		for (Pair<HRegionInfo,ServerName> info: regions) {
			if (isRegionInScan(scan,info.getFirst())) {
				if (LOG.isTraceEnabled())
					SpliceLogUtils.trace(LOG, "regionInfo with encodedname {%s} and region name as string %s", info.getFirst().getEncodedName(), info.getFirst().getRegionNameAsString());				
				numberOfRegionsInvolved++;
				rowCount+=getRowSize(constantRowSize,regionLoads==null?null:regionLoads.get(info.getFirst().getRegionNameAsString()),hfileMaxSize);	
			}
		}
		if (numberOfRegionsInvolved == 1 && scan.getStartRow() != null && !Bytes.equals(scan.getStartRow(),HConstants.EMPTY_START_ROW) && scan.getStopRow() != null && !Bytes.equals(scan.getStopRow(),HConstants.EMPTY_END_ROW) ) {
			rowCount=(long) ( ( (double)rowCount)*SpliceConstants.extraStartStopQualifierMultiplier);
		}
		return rowCount;
	}

	protected static boolean isRegionInScan(Scan scan, HRegionInfo regionInfo) {
		assert (scan != null);
		assert (regionInfo != null);
		return BytesUtil.overlap(regionInfo.getStartKey(), regionInfo.getEndKey(), scan.getStartRow(), scan.getStopRow());
	}
	
	protected static long getRowSize(long constantRowSize, RegionLoad regionLoad, long hfileMaxSize) {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "getRowSize with constantRowSize %d and regionLoad %s and hfileMaxSize %d",constantRowSize, regionLoad, hfileMaxSize);
		if (regionLoad==null)
			return constantRowSize; // No Metrics
		float rowSize = (float) constantRowSize*((float) HBaseRegionLoads.memstoreAndStorefileSize(regionLoad)/(float) hfileMaxSize);
		return rowSize < SpliceConstants.optimizerTableMinimalRows?SpliceConstants.optimizerTableMinimalRows:(long) rowSize;
	}
	
    public static SortedSet<Pair<HRegionInfo,ServerName>> getRegions(long conglomId) {
        String table = Long.toString(conglomId);
        try {
            return HBaseRegionCache.getInstance().getRegions(Bytes.toBytes(table));
        } catch (ExecutionException e) {
        	SpliceLogUtils.error(LOG, "Erorr in getRegions on the cost controller, should not happen", e);
        	return null;
        }
    }
    /**
     * Scratch Estimate...
     */
	@Override
	public long getEstimatedRowCount() throws StandardException {
		return 0;
	}
	@Override
	public void extraQualifierSelectivity(CostEstimate costEstimate) throws StandardException {
		if (LOG.isTraceEnabled())
			SpliceLogUtils.trace(LOG, "extraQualifierSelectivity costEstimate=%s",costEstimate);
		costEstimate.setCost(costEstimate.getEstimatedCost()*SpliceConstants.extraQualifierMultiplier, 
				(double) costEstimate.getEstimatedRowCount()*SpliceConstants.extraQualifierMultiplier, 
				costEstimate.singleScanRowCount()*SpliceConstants.extraQualifierMultiplier);
	};
	
}
