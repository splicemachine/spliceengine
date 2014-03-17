package com.splicemachine.derby.impl.job.AlterTable;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 2/6/14
 * Time: 9:22 PM
 * To change this template use File | Settings | File Templates.
 */

import java.io.IOException;
import java.util.List;

import com.google.common.collect.Lists;
import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.derby.impl.sql.execute.ValueRow;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.log4j.Logger;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.stats.MetricFactory;
import com.splicemachine.stats.Metrics;
import com.splicemachine.stats.TimeView;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.utils.SpliceLogUtils;

public class ConglomerateScanner {

    private static Logger LOG = Logger.getLogger(ConglomerateScanner.class);
    private ColumnInfo[] columnInfo;
    private String txnId;
    private String xplainSchema;

    private ExecRow row;
    private HRegion region;
    private BufferedRegionScanner brs;
    private EntryDecoder entryDecoder;

    public ConglomerateScanner(ColumnInfo[] columnInfo,
                               HRegion region,
                               String txnId,
                               String xplainSchema) throws StandardException{
        this.columnInfo = columnInfo;
        this.xplainSchema = xplainSchema;
        this.txnId = txnId;
        this.region = region;
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

    private void initScanner() {

        // initialize a region scanner
        Scan regionScan = SpliceUtils.createScan(txnId);
        regionScan.setCaching(SpliceConstants.DEFAULT_CACHE_SIZE);
        regionScan.setStartRow(region.getStartKey());
        regionScan.setStopRow(region.getEndKey());
        regionScan.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY);

        MetricFactory metricFactory = xplainSchema!=null? Metrics.basicMetricFactory(): Metrics.noOpMetricFactory();
        try{
            //Scan previously committed data
            RegionScanner sourceScanner = region.getCoprocessorHost().preScannerOpen(regionScan);
            if(sourceScanner==null)
                sourceScanner = region.getScanner(regionScan);
            Scan scan = SpliceUtils.createScan(txnId);
            brs = new BufferedRegionScanner(region,sourceScanner,scan,SpliceConstants.DEFAULT_CACHE_SIZE,metricFactory);
        } catch (IOException e) {
            //TODO: handle exceptions
            SpliceLogUtils.error(LOG, e);
            //throw new ExecutionException(e);
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, e);
            //throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

    public List<Cell> next() {
        if (brs == null) {
            initScanner();
        }

        if(entryDecoder==null) {
            entryDecoder = new EntryDecoder(SpliceDriver.getKryoPool());
        }

        List<Cell> nextRow = Lists.newArrayListWithExpectedSize(16);
        boolean more = true;
        try {
            nextRow.clear();
            more = brs.nextRaw(nextRow);
            if (!more) return null;

        } catch (Exception e) {
            // FIXME: jc - should we be swallowing this?
        }
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
