package com.splicemachine.derby.impl.store.access.base;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.store.access.StoreCostController;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.primitives.Bytes;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import com.splicemachine.derby.hbase.DerbyFactory;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;

public abstract class SpliceGenericCostController implements StoreCostController {
    private static final Logger LOG = Logger.getLogger(SpliceGenericCostController.class);
    protected static DerbyFactory derbyFactory = DerbyFactoryDriver.derbyFactory;

    public static boolean isRegionInScan(Scan scan, HRegionInfo regionInfo) {
        assert (scan != null);
        assert (regionInfo != null);
        return Bytes.overlap(regionInfo.getStartKey(), regionInfo.getEndKey(), scan.getStartRow(), scan.getStopRow());
    }

    /**
     * Scratch Estimate...
     */
    @Override
    public long getEstimatedRowCount() throws StandardException {
        return 0;
    }

    @Override
    public RowLocation newRowLocationTemplate() throws StandardException {
        return null;
    }
}
