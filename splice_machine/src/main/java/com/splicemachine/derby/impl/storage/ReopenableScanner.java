package com.splicemachine.derby.impl.storage;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.si.impl.SIFactoryDriver;
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
    protected static final SDataLib dataLib = SIFactoryDriver.siFactory.getDataLib();
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
                SpliceLogUtils.trace(LOG, "Re-create scanner with startRow = %s", Bytes.toHex(lastRow));
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

        if (lastRow != null) {
            scan.setStartRow(lastRow);
        }
        delegate = new BufferedRegionScanner(region, region.getScanner(scan), scan, SpliceConstants.DEFAULT_CACHE_SIZE, metricFactory,HTransactorFactory.getTransactor().getDataLib() );
        //skip the first row because it has been seen
        try {
            if (lastRow != null) {
                delegate.next();
            }
        } catch (IOException e) {
            if (Exceptions.isScannerTimeoutException(e) && getNumRetries() < MAX_RETIRES) {
                SpliceLogUtils.trace(LOG, "Re-create scanner with startRow = %s", Bytes.toHex(lastRow));
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
