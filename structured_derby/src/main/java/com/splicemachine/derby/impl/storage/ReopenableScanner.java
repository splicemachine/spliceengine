package com.splicemachine.derby.impl.storage;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.stats.MetricFactory;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;
import org.apache.hadoop.hbase.regionserver.HRegion;
import java.io.IOException;

/**
 * Created by jyuan on 3/27/14.
 */
public abstract class ReopenableScanner {
    private static Logger LOG = Logger.getLogger(ReopenableScanner.class);
    public static final int MAX_RETIRES = 10;
    private byte[] lastRow;
    private int numRetries;

    public ReopenableScanner() {
        numRetries = 0;
        lastRow = null;
    }

    public byte[] getLastRow() {
        return lastRow;
    }

    public void setLastRow(byte[] row) {
        lastRow = row;
    }

    public int getNumRetries() {return numRetries;}

    public void incrementNumRetries() {numRetries++;}

    public ResultScanner reopenResultScanner(ResultScanner delegate, Scan scan, HTableInterface htable) throws IOException{
        delegate.close();

        if (lastRow != null) {
            scan.setStartRow(lastRow);
        }
        delegate = htable.getScanner(scan);
        //skip the first row because it has been seen
        try {
            if (lastRow != null) {
                delegate.next();
            }
        } catch (IOException e) {
            if (Exceptions.isScannerTimeoutException(e) && getNumRetries() < MAX_RETIRES) {
                SpliceLogUtils.trace(LOG, "Re-create scanner with startRow = %s", BytesUtil.toHex(lastRow));
                incrementNumRetries();
                delegate = reopenResultScanner(delegate, scan, htable);
            }
            else {
                SpliceLogUtils.logAndThrowRuntime(LOG, e);
            }
        }
        return delegate;
    }

    public MeasuredRegionScanner reopenRegionScanner(MeasuredRegionScanner delegate, HRegion region, Scan scan, MetricFactory metricFactory) throws IOException {
        delegate.close();
        if (lastRow != null) {
            scan.setStartRow(lastRow);
        }
        delegate = new BufferedRegionScanner(region, region.getScanner(scan), scan, SpliceConstants.DEFAULT_CACHE_SIZE, metricFactory);
        //skip the first row because it has been seen
        try {
            if (lastRow != null) {
                delegate.next();
            }
        } catch (IOException e) {
            if (Exceptions.isScannerTimeoutException(e) && getNumRetries() < MAX_RETIRES) {
                SpliceLogUtils.trace(LOG, "Re-create scanner with startRow = %s", BytesUtil.toHex(lastRow));
                incrementNumRetries();
                delegate = reopenRegionScanner(delegate, region, scan, metricFactory);
            }
            else {
                SpliceLogUtils.logAndThrowRuntime(LOG, e);
            }
        }

        return delegate;
    }
}
