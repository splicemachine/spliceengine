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
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.hbase.MeasuredResultScanner;
import com.splicemachine.hbase.BaseReadAheadRegionScanner;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.Txn;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.si.impl.DDLTxnView;
import com.splicemachine.si.impl.SIFilterPacked;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.si.impl.TxnFilter;
import com.splicemachine.si.impl.rollforward.SegmentedRollForward;
import com.splicemachine.storage.EntryDecoder;
import com.splicemachine.storage.EntryPredicateFilter;
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
    private Txn txn;
    private boolean isTraced;

    private HRegion region;
    private final byte[] scanStart;
    private final byte[] scanFinish;
    private MeasuredRegionScanner brs;
    private EntryDecoder entryDecoder;
    private final long demarcationTimestamp;
    private List<KeyValue> values;

    public ConglomerateScanner(HRegion region,
                               Txn txn,
                               long demarcationTimestamp,
                               boolean isTraced,
                               byte[] scanStart,
                               byte[] scanFinish) throws StandardException{
        this.txn = txn;
        this.isTraced = isTraced;
        this.region = region;
        this.scanStart = scanStart;
        this.scanFinish = scanFinish;
        this.demarcationTimestamp = demarcationTimestamp;
    }

    private void initScanner() throws ExecutionException{

        // initialize a region scanner
        Scan regionScan = SpliceUtils.createScan(txn);
        regionScan.setCaching(SpliceConstants.DEFAULT_CACHE_SIZE);
        regionScan.setStartRow(scanStart);
        regionScan.setStopRow(scanFinish);
        regionScan.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES, SpliceConstants.PACKED_COLUMN_BYTES);

        MetricFactory metricFactory = isTraced? Metrics.basicMetricFactory(): Metrics.noOpMetricFactory();
        try{
            //Scan previously committed data
            brs = getRegionScanner(txn,demarcationTimestamp,regionScan,metricFactory);
        } catch (IOException e) {
            SpliceLogUtils.error(LOG, e);
            throw new ExecutionException(e);
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, e);
            throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

    private MeasuredRegionScanner getRegionScanner(Txn txn, long demarcationTimestamp,Scan regionScan, MetricFactory metricFactory) throws IOException {
        //manually create the SIFilter
        DDLTxnView demarcationPoint = new DDLTxnView(txn, demarcationTimestamp);
        TransactionalRegion transactionalRegion = TransactionalRegions.get(region);
        TxnFilter packed = transactionalRegion.packedFilter(demarcationPoint, EntryPredicateFilter.emptyPredicate(), false);
        transactionalRegion.discard();
        regionScan.setFilter(new SIFilterPacked(packed));
        RegionScanner sourceScanner = region.getScanner(regionScan);
        return SpliceConstants.useReadAheadScanner? new BaseReadAheadRegionScanner(region, SpliceConstants.DEFAULT_CACHE_SIZE, sourceScanner,metricFactory)
                : new BufferedRegionScanner(region,sourceScanner,regionScan,SpliceConstants.DEFAULT_CACHE_SIZE,SpliceConstants.DEFAULT_CACHE_SIZE,metricFactory);
    }

    public List<KeyValue> next() throws ExecutionException, IOException{
        if (brs == null) {
            initScanner();
        }

        if(entryDecoder==null) {
            entryDecoder = new EntryDecoder();
        }

        if(values==null)
            values = Lists.newArrayListWithCapacity(4);

        boolean more;

        values.clear();
        more = brs.nextRaw(values, null);
        if (!more) return null;

        return values;
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
