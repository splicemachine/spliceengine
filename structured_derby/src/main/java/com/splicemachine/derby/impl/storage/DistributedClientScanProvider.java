package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.RowKeyDistributorByHashPrefix;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.marshall.BucketHasher;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.stats.TimeView;
import com.splicemachine.utils.SpliceLogUtils;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

/**
 * RowProvider which uses an HBase client ResultScanner to
 * pull rows in serially.
 *
 * @author Scott Fines
 *         Date Created: 1/17/13:1:23 PM
 */
public class DistributedClientScanProvider extends AbstractMultiScanProvider {
    private static final Logger LOG = Logger.getLogger(DistributedClientScanProvider.class);
    private final TableName tableName;
    private HTableInterface htable;
    private final Scan scan;
    private final RowKeyDistributorByHashPrefix keyDistributor;

    private SpliceResultScanner scanner;


    public DistributedClientScanProvider(String type,
                                         TableName tableName,
                                         Scan scan,
                                         PairDecoder decoder,
                                         SpliceRuntimeContext spliceRuntimeContext) {
        super(decoder, type, spliceRuntimeContext);
        SpliceLogUtils.trace(LOG, "instantiated");
        this.tableName = tableName;
        this.scan = scan;
        this.keyDistributor = new RowKeyDistributorByHashPrefix(BucketHasher.getHasher(SpliceDriver.driver()
                                                                                                   .getTempTable()
                                                                                                   .getCurrentSpread
                                                                                                       ()));
    }

    @Override
    public Result getResult() throws StandardException {
        try {
            return scanner.next();
        } catch (IOException e) {
            SpliceLogUtils.logAndThrow(LOG, "Unable to getResult", Exceptions.parseException(e));
            return null;//won't happen
        }
    }

    @Override
    public void open() {
        SpliceLogUtils.trace(LOG, "open");
        if (htable == null)
            htable = SpliceAccessManager.getHTable(tableName);
        try {
            scanner = DistributedScanner.create(htable, scan, keyDistributor, spliceRuntimeContext);
        } catch (IOException e) {
            SpliceLogUtils.logAndThrowRuntime(LOG, "unable to open table " + tableName.getNameAsString(), e);
        }
    }

    @Override
    public void reportStats(long statementId, long operationId, long taskId, String xplainSchema, String regionName) {
        if (regionName == null)
            regionName = "ControlRegion";
        OperationRuntimeStats stats = new OperationRuntimeStats(statementId, operationId, taskId, regionName, 5);

        stats.addMetric(OperationMetric.REMOTE_SCAN_BYTES, scanner.getRemoteBytesRead());
        stats.addMetric(OperationMetric.REMOTE_SCAN_ROWS, scanner.getRemoteRowsRead());
        TimeView remoteTime = scanner.getRemoteReadTime();
        stats.addMetric(OperationMetric.REMOTE_SCAN_WALL_TIME, remoteTime.getWallClockTime());
        stats.addMetric(OperationMetric.REMOTE_SCAN_CPU_TIME, remoteTime.getCpuTime());
        stats.addMetric(OperationMetric.REMOTE_SCAN_USER_TIME, remoteTime.getUserTime());

        TimeView view = timer.getTime();
        stats.addMetric(OperationMetric.TOTAL_WALL_TIME, view.getWallClockTime());
        stats.addMetric(OperationMetric.TOTAL_CPU_TIME, view.getCpuTime());
        stats.addMetric(OperationMetric.TOTAL_USER_TIME, view.getUserTime());
        stats.addMetric(OperationMetric.START_TIMESTAMP, startExecutionTime);
        stats.addMetric(OperationMetric.STOP_TIMESTAMP, stopExecutionTime);
        stats.addMetric(OperationMetric.OUTPUT_ROWS, timer.getNumEvents());
        stats.addMetric(OperationMetric.INPUT_ROWS, scanner.getRemoteRowsRead());

        SpliceDriver.driver().getTaskReporter().report(xplainSchema, stats);
    }

    @Override
    public void close() {
        super.close();
        SpliceLogUtils.trace(LOG, "closed after calling hasNext %d times", called);
        if (scanner != null) scanner.close();
        if (htable != null)
            try {
                htable.close();
            } catch (IOException e) {
                SpliceLogUtils.logAndThrowRuntime(LOG, "unable to close htable for " + tableName.getNameAsString(), e);
            }
    }

    @Override
    public List<Scan> getScans() throws StandardException {
        try {
            return Arrays.asList(keyDistributor.getDistributedScans(scan));
        } catch (IOException e) {
            throw Exceptions.parseException(e);
        }
    }

    @Override
    public TableName getTableName() {
        return tableName;
    }

    @Override
    public SpliceRuntimeContext getSpliceRuntimeContext() {
        return spliceRuntimeContext;
    }

}
