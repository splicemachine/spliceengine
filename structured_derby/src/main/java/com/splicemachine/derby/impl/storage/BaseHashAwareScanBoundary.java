package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.iapi.storage.ScanBoundary;
import com.splicemachine.derby.utils.SpliceUtils;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

/**
 * Abstract implementation of a ScanBoundary which does skip-forward and look-aheads
 * as needed. That is, this defines a Scan boundary that will skip past rows at the beginning
 * of a region until reaching the first key prefix fully contained in the region, and will
 * read on past the end of a region until a key prefix is exhausted.
 *
 * @author Scott Fines
 * Created: 1/17/13 9:39 PM
 */
public abstract class BaseHashAwareScanBoundary implements ScanBoundary{
    private static final Logger LOG = Logger.getLogger(BaseHashAwareScanBoundary.class);
    private final byte[] columnFamily;

    protected BaseHashAwareScanBoundary(byte[] columnFamily) {
        this.columnFamily=columnFamily;
    }

    @Override
    public Scan buildScan(String transactionId, byte[] start, byte[] finish) {
        Scan scan = SpliceUtils.createScan(transactionId);
        scan.setStartRow(start);
        scan.setStopRow(finish);
        scan.addFamily(columnFamily);
        scan.setCaching(100);
        return scan;
    }

    @Override
    public boolean shouldLookBehind(byte[] firstRowInRegion) {
        //don't look behind, we only skip forward
        return false;
    }

    @Override
    public boolean shouldStartLate(byte[] firstRowInRegion) {
        //we skip forward if this is called
        return true;
    }

    @Override
    public boolean shouldStopEarly(byte[] firstRowInNextRegion) {
        //always look ahead
        return false;
    }

    @Override
    public boolean shouldLookAhead(byte[] firstRowInNextRegion) {
        return true;
    }

}
