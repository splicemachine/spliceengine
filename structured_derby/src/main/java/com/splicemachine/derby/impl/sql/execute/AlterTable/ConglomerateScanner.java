package com.splicemachine.derby.impl.sql.execute.AlterTable;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 2/6/14
 * Time: 9:22 PM
 * To change this template use File | Settings | File Templates.
 */

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.stats.MetricFactory;
import com.splicemachine.stats.Metrics;
import com.splicemachine.stats.TimeView;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

public class ConglomerateScanner {

    private static Logger LOG = Logger.getLogger(ConglomerateScanner.class);
    private ColumnInfo[] columnInfo;
    private String txnId;
    private String xplainSchema;

    private ExecRow row;
    private HRegion region;
    private final byte[] scanStart;
    private final byte[] scanFinish;
    private BufferedRegionScanner brs;
    private EntryDecoder entryDecoder;

    public ConglomerateScanner(ColumnInfo[] columnInfo,
                               HRegion region,
                               String txnId,
                               String xplainSchema,
                               byte[] scanStart,
                               byte[] scanFinish) throws StandardException{
        this.columnInfo = columnInfo;
        this.xplainSchema = xplainSchema;
        this.txnId = txnId;
        this.region = region;
        this.scanStart = scanStart;
        this.scanFinish = scanFinish;
        initExecRow();
    }

    private void initExecRow() throws StandardException{

        // initialize ExecRow
        row = new ValueRow(columnInfo.length);
        for (int i = 0; i < columnInfo.length; ++i){
            DataValueDescriptor dataValue = columnInfo[i].dataType.getNull();
            row.setColumn(i+1 ,dataValue);
        }
    }

    private void initScanner() throws ExecutionException{

        // initialize a region scanner
        Scan regionScan = SpliceUtils.createScan(txnId);
        regionScan.setCaching(SpliceConstants.DEFAULT_CACHE_SIZE);
        regionScan.setStartRow(scanStart);
        regionScan.setStopRow(scanFinish);
        regionScan.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES, SpliceConstants.PACKED_COLUMN_BYTES);

        MetricFactory metricFactory = xplainSchema!=null? Metrics.basicMetricFactory(): Metrics.noOpMetricFactory();
        try{
            //Scan previously committed data
            RegionScanner sourceScanner = region.getCoprocessorHost().preScannerOpen(regionScan);
            if(sourceScanner==null)
                sourceScanner = region.getScanner(regionScan);
            Scan scan = SpliceUtils.createScan(txnId);
            brs = new BufferedRegionScanner(region,sourceScanner,scan,SpliceConstants.DEFAULT_CACHE_SIZE,metricFactory);
        } catch (IOException e) {
            SpliceLogUtils.error(LOG, e);
            throw new ExecutionException(e);
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, e);
            throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

    public List<KeyValue> next() throws ExecutionException, IOException{
        if (brs == null) {
            initScanner();
        }

        if(entryDecoder==null) {
            entryDecoder = new EntryDecoder();
        }

        List<KeyValue> nextRow = Lists.newArrayListWithExpectedSize(16);
        boolean more = true;

        nextRow.clear();
        more = brs.nextRaw(nextRow, null);
        if (!more) return null;

        return nextRow;
    }

    public TimeView getReadTime() {
        return brs.getReadTime();
    }

    public long getBytesOutput() {
        return brs.getBytesOutput();
    }

    public long getRowsOutput() {
        return brs.getRowsOutput();
    }
}
