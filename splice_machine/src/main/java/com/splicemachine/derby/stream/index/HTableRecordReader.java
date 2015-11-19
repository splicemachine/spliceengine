package com.splicemachine.derby.stream.index;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.hbase.DerbyFactoryDriver;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.hbase.SimpleMeasuredRegionScanner;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.SMSQLUtil;
import com.splicemachine.mrio.api.core.SMSplit;
import com.splicemachine.mrio.api.core.SpliceRegionScanner;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.mapreduce.TableSplit;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * Created by jyuan on 10/19/15.
 */
public class HTableRecordReader extends RecordReader<byte[], KVPair> {
    protected static final Logger LOG = Logger.getLogger(HTableRecordReader.class);
    protected Table htable;
    protected HRegion hregion;
    protected Configuration config;
    protected MeasuredRegionScanner mrs;
    protected HTableScanner hTableScanner;
    protected long txnId;
    protected Scan scan;
    protected SMSQLUtil sqlUtil = null;
    protected KVPair currentRow;
    protected HTableScannerBuilder builder;
    protected byte[] rowKey;

    public HTableRecordReader(Configuration config) {
        this.config = config;
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "initialize with split=%s", split);
        init(config==null?context.getConfiguration():config,split);
    }

    public void init(Configuration config, InputSplit split) throws IOException, InterruptedException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "init");
        String tableScannerAsString = config.get(MRConstants.SPLICE_SCAN_INFO);
        if (tableScannerAsString == null)
            throw new IOException("splice scan info was not serialized to task, failing");
        try {
            builder = HTableScannerBuilder.getTableScannerBuilderFromBase64String(tableScannerAsString);
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "config loaded builder=%s",builder);
            TableSplit tSplit = ((SMSplit)split).getSplit();
            Scan scan = builder.getScan();
            scan.setStartRow(tSplit.getStartRow());
            scan.setStopRow(tSplit.getEndRow());
            this.scan = scan;
            restart(tSplit.getStartRow());
        } catch (StandardException e) {
            throw new IOException(e);
        }
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        try {
            KVPair nextRow = hTableScanner.next();
            if (nextRow != null) {
                currentRow = nextRow;
                rowKey = nextRow.getRowKey();
            } else {
                currentRow = null;
            }
            return currentRow != null;
        } catch (StandardException e) {
            throw new IOException(e);
        }
    }

    @Override
    public byte[] getCurrentKey() throws IOException, InterruptedException {
        return rowKey;
    }

    @Override
    public KVPair getCurrentValue() throws IOException, InterruptedException {
        return currentRow;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return 0;
    }

    @Override
    public void close() throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "close");
        try {
            hTableScanner.close();

        } catch (StandardException e) {
            throw new IOException(e);
        }
    }

    public void setScan(Scan scan) {
        this.scan = scan;
    }

    public void setHTable(Table htable) {
        this.htable = htable;
    }

    public void restart(byte[] firstRow) throws IOException {
        Scan newscan = new Scan(scan);
        newscan.setStartRow(firstRow);
        scan = newscan;
        if(htable != null) {
            SpliceRegionScanner splitRegionScanner = DerbyFactoryDriver.derbyFactory.getSplitRegionScanner(scan,htable);
            this.hregion = splitRegionScanner.getRegion();
            this.mrs = new SimpleMeasuredRegionScanner(splitRegionScanner, Metrics.noOpMetricFactory());
            builder.tableVersion("2.0")
                    .region(TransactionalRegions.get(hregion))
                    .scanner(mrs)
                    .scan(scan)
                    .metricFactory(Metrics.noOpMetricFactory());
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "restart with builder=%s",builder);
            hTableScanner = builder.build();
        } else {
            throw new IOException("htable not set");
        }
    }
}
