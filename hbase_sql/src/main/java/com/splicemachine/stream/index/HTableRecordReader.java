package com.splicemachine.stream.index;

import com.splicemachine.access.hbase.HBaseConnectionFactory;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.derby.impl.SpliceSpark;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.stream.iterator.DirectScanner;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.mrio.MRConstants;
import com.splicemachine.mrio.api.core.SMSplit;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
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
public class HTableRecordReader extends RecordReader<byte[], KVPair>{
    protected static final Logger LOG = Logger.getLogger(HTableRecordReader.class);
    protected Table htable;
    protected HRegion hregion;
    protected Configuration config;
    protected DirectScanner directScanner;
    protected Scan scan;
    protected KVPair currentRow;
    protected TableScannerBuilder builder;
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
            SpliceSpark.setupSpliceStaticComponents();
            builder =(TableScannerBuilder)HTableScannerBuilder.getTableScannerBuilderFromBase64String(tableScannerAsString);
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "config loaded builder=%s",builder);
            TableSplit tSplit = ((SMSplit)split).getSplit();
            Scan scan = ((HScan)builder.getScan()).unwrapDelegate();
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
            KVPair nextRow = directScanner.next();
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
            directScanner.close();

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
            SIDriver driver=SIDriver.driver();
            HBaseConnectionFactory instance=HBaseConnectionFactory.getInstance(driver.getConfiguration());
            Clock clock = driver.getClock();
            Partition clientPartition = new ClientPartition(instance.getConnection(),htable.getName(),htable,clock,driver.getPartitionInfoCache());
            SplitRegionScanner srs = new SplitRegionScanner(scan,
                    htable,
                    instance.getConnection(),
                    clock,
                    clientPartition);
            this.hregion = srs.getRegion();

            long conglomId = Long.parseLong(hregion.getTableDesc().getTableName().getQualifierAsString());
            RegionPartition basePartition=new RegionPartition(hregion);

            directScanner= new DirectScanner(new RegionDataScanner(basePartition,srs,Metrics.basicMetricFactory()),
                    driver.transactionalPartition(conglomId,basePartition),
                    builder.getTxn(), //TODO -sf- might not be correct
                    builder.getTxn().getBeginTimestamp(),
                    Metrics.basicMetricFactory());
        } else {
            throw new IOException("htable not set");
        }
    }
}
