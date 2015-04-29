package com.splicemachine.derby.impl.storage;

import com.splicemachine.async.*;
import com.splicemachine.async.AsyncScanner;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.hbase.regioninfocache.HBaseRegionCache;
import com.splicemachine.hbase.regioninfocache.RegionCache;
import com.splicemachine.hbase.ScanDivider;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.db.iapi.error.StandardException;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.List;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 7/18/14
 */
public class AsyncClientScanProvider extends AbstractAsyncScanProvider {
    private static final Logger LOG = Logger.getLogger(ClientScanProvider.class);
    private final byte[] tableName;
    private final Scan scan;
    private AsyncScanner scanner;
    private long stopExecutionTime;
    private long startExecutionTime;
    private final RegionCache regionCache;
    private final HBaseClient hbaseClient;

    private boolean opened = false;

    public AsyncClientScanProvider(String type,
                              byte[] tableName, Scan scan,
                              PairDecoder decoder,
                              SpliceRuntimeContext spliceRuntimeContext) {
        super(decoder, type, spliceRuntimeContext);
        SpliceLogUtils.trace(LOG, "instantiated");
        this.tableName = tableName;
        this.scan = scan;
        this.regionCache = HBaseRegionCache.getInstance();
        this.hbaseClient = AsyncHbase.HBASE_CLIENT;
    }

    @Override
    public List<KeyValue> getResult() throws StandardException, IOException {
        if(!opened) {
            scanner.open();
            opened=true;
        }
        try {
            return scanner.nextKeyValues();
        } catch (Exception e) {
            SpliceLogUtils.warn(LOG,"AsyncClientScanProvider#getResult handled an exception",e);
            throw Exceptions.parseException(e);
        }
    }

    private static final int PARALLEL_SCAN_REGION_THRESHOLD = 2; //TODO -sf- make this configurable
    @Override
    public void open() {
        SpliceLogUtils.trace(LOG, "open");
        try {
            SortedSet<Pair<HRegionInfo,ServerName>> regions = regionCache.getRegionsInRange(tableName, scan.getStartRow(), scan.getStopRow());
            if (regions.size() < PARALLEL_SCAN_REGION_THRESHOLD) {
                scanner = new QueueingAsyncScanner(DerbyAsyncScannerUtils.convertScanner(scan, tableName, hbaseClient), spliceRuntimeContext);
            } else {
                List<Scanner> scanners = DerbyAsyncScannerUtils.convertScanners(ScanDivider.divide(scan, regions), this.tableName, hbaseClient, true);
                scanner = new SortedMultiScanner(scanners,SpliceConstants.DEFAULT_CACHE_SIZE*3,Bytes.BYTES_COMPARATOR,spliceRuntimeContext);
            }

        } catch (IOException | ExecutionException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, "unable to open table " + Bytes.toString(tableName), e);
        }
        startExecutionTime = System.currentTimeMillis();
    }

    @Override
    public void close() throws StandardException {
        super.close();
        SpliceLogUtils.trace(LOG, "closed after calling hasNext %d times",called);
        if(scanner!=null)scanner.close();

        stopExecutionTime = System.currentTimeMillis();
    }

    @Override
    public Scan toScan() {
        return scan;
    }

    @Override
    public byte[] getTableName() {
        return tableName;
    }

}
