package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.async.AsyncScanner;
import com.splicemachine.async.SimpleAsyncScanner;
import com.splicemachine.metrics.BaseIOStats;
import com.splicemachine.metrics.IOStats;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.async.KeyValue;

import java.io.IOException;
import java.util.List;

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

    public AsyncClientScanProvider(String type,
                              byte[] tableName, Scan scan,
                              PairDecoder decoder,
                              SpliceRuntimeContext spliceRuntimeContext) {
        super(decoder, type, spliceRuntimeContext);
        SpliceLogUtils.trace(LOG, "instantiated");
        this.tableName = tableName;
        this.scan = scan;
    }

    @Override
    public List<KeyValue> getResult() throws StandardException, IOException {
        try {
            return scanner.nextKeyValues();
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public void open() {
        SpliceLogUtils.trace(LOG, "open");
        try {
            scanner = new SimpleAsyncScanner(DerbyAsyncScannerUtils.convertScanner(scan,tableName,SimpleAsyncScanner.HBASE_CLIENT),spliceRuntimeContext);
//            scanner = GatheringScanner.newScanner(tableName,scan,spliceRuntimeContext);
            scanner.open();
        } catch (IOException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG,"unable to open table "+ Bytes.toString(tableName),e);
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

    @Override
    public void reportStats(long statementId, long operationId, long taskId, String xplainSchema,String regionName) throws IOException {
        if(regionName==null)
            regionName="ControlRegion";
        OperationRuntimeStats stats = new OperationRuntimeStats(statementId,operationId,taskId,regionName,8);
        stats.addMetric(OperationMetric.REMOTE_SCAN_BYTES,scanner.getRemoteBytesRead());
        stats.addMetric(OperationMetric.REMOTE_SCAN_ROWS,scanner.getRemoteRowsRead());
        TimeView remoteView = scanner.getRemoteReadTime();
        stats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME,remoteView.getWallClockTime());
        stats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME,remoteView.getCpuTime());
        stats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME,remoteView.getUserTime());
        stats.addMetric(OperationMetric.TOTAL_WALL_TIME,remoteView.getWallClockTime());
        stats.addMetric(OperationMetric.TOTAL_CPU_TIME,remoteView.getCpuTime());
        stats.addMetric(OperationMetric.TOTAL_USER_TIME,remoteView.getUserTime());
        stats.addMetric(OperationMetric.OUTPUT_ROWS,scanner.getRemoteRowsRead());
        stats.addMetric(OperationMetric.INPUT_ROWS,scanner.getRemoteRowsRead());
        stats.addMetric(OperationMetric.START_TIMESTAMP,startExecutionTime);
        stats.addMetric(OperationMetric.STOP_TIMESTAMP,stopExecutionTime);

        SpliceDriver.driver().getTaskReporter().report(stats,spliceRuntimeContext.getTxn());
    }

    @Override
    public IOStats getIOStats() {
        return new BaseIOStats(scanner.getRemoteReadTime(),scanner.getRemoteBytesRead(),scanner.getRemoteRowsRead());
    }
}
