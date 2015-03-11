package com.splicemachine.derby.impl.store.access.base;

import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.compile.CostEstimate;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.store.access.conglomerate.GenericCostController;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.hbase.regioninfocache.HBaseRegionCache;
import com.splicemachine.utils.SpliceLogUtils;

public abstract class SpliceGenericCostController extends GenericCostController {
    private static final Logger LOG = Logger.getLogger(SpliceGenericCostController.class);
    protected static DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;

    public static boolean isRegionInScan(Scan scan, HRegionInfo regionInfo) {
        assert (scan != null);
        assert (regionInfo != null);
        return BytesUtil.overlap(regionInfo.getStartKey(), regionInfo.getEndKey(), scan.getStartRow(), scan.getStopRow());
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

    @Override
    public RowLocation newRowLocationTemplate() throws StandardException {
        return null;
    }
}
