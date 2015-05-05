package com.splicemachine.derby.impl.job.altertable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Throwables;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.SITableScanner;
import com.splicemachine.derby.impl.sql.execute.operations.scanner.TableScannerBuilder;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.hbase.ReadAheadRegionScanner;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.api.RowTransformer;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.ddl.TransformingDDLDescriptor;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.DDLTxnView;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;

/**
 * @author Jeff Cunningham
 *         Date: 3/19/15
 */
public class PopulateConglomerateTask extends ZkTask {
    private static final long serialVersionUID = 1L;

    private int expectedScanReadWidth;
    private long demarcationTimestamp;
    private DDLChange ddlChange;

    private RecordingCallBuffer<KVPair> writeBuffer;
    private RowTransformer transformer;
    private SITableScanner scanner;
    private SpliceRuntimeContext runtimeContext;

    private byte[] scanStart;
    private byte[] scanStop;
    private static SDataLib dataLib = SIFactoryDriver.siFactory.getDataLib();

    public PopulateConglomerateTask() { }

    public PopulateConglomerateTask(String jobId,
                                    int expectedScanReadWidth,
                                    long demarcationTimestamp,
                                    DDLChange ddlChange) {
        super(jobId, OperationJob.operationTaskPriority,null);
        this.expectedScanReadWidth = expectedScanReadWidth;
        this.demarcationTimestamp = demarcationTimestamp;
        this.ddlChange = ddlChange;
    }

    @Override
    public RegionTask getClone() {
        return new PopulateConglomerateTask(jobId, expectedScanReadWidth, demarcationTimestamp, ddlChange);
    }

    @Override
    public boolean isSplittable() {
        return true;
    }

    @Override
    public void prepareTask(byte[] start, byte[] end,RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper)
        throws ExecutionException {
        this.region = rce.getRegion();
        super.prepareTask(start,end,rce, zooKeeper);
        this.scanStart = start;
        this.scanStop = end;
        this.writeBuffer = null;
        this.transformer = null;
        this.scanner = null;
    }

    @Override
    protected String getTaskType() {
        return getClass().getSimpleName();
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }

    @Override
    public void doExecute() throws ExecutionException {
        try {
            try (SITableScanner scanner = getTableScanner(Metrics.noOpMetricFactory())) {
                ExecRow nextRow = scanner.next(runtimeContext);
                while (nextRow != null) {
                    transformResults(nextRow, getTransformer(), getWriteBuffer());
                    nextRow = scanner.next(runtimeContext);
                }
            } finally {
                if (writeBuffer != null) {
                    writeBuffer.flushBuffer();
                    writeBuffer.close();
                }
            }
        } catch (IOException e) {
            SpliceLogUtils.error(LOG, e);
            throw new ExecutionException(e);
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, e);
            throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

    private SITableScanner getTableScanner(MetricFactory metricFactory) throws IOException {
        if (scanner == null) {
            Txn txn = getTxn();
            Scan scan = SpliceUtils.createScan(txn);
            scan.setCaching(SpliceConstants.DEFAULT_CACHE_SIZE);
            scan.addFamily(SIConstants.DEFAULT_FAMILY_BYTES);
            scan.setStartRow(scanStart);
            scan.setStopRow(scanStop);
            scan.setCacheBlocks(false);
            scan.setMaxVersions();

            RegionScanner regionScanner = region.getScanner(scan);

            MeasuredRegionScanner mrs = SpliceConstants.useReadAheadScanner ?
                                                            new ReadAheadRegionScanner(region,
                                                                                       SpliceConstants.DEFAULT_CACHE_SIZE,
                                                                                       regionScanner,metricFactory,
                                                                                       dataLib)
                                                            : new BufferedRegionScanner(region,
                                                                                        regionScanner,
                                                                                        scan,
                                                                                        SpliceConstants.DEFAULT_CACHE_SIZE,
                                                                                        SpliceConstants.DEFAULT_CACHE_SIZE,
                                                                                        metricFactory,
                                                                                        dataLib);

            TxnView txnView = new DDLTxnView(txn, this.demarcationTimestamp);

            TableScannerBuilder builder = new TableScannerBuilder();
            builder.region(TransactionalRegions.get(region)).scan(scan).scanner(mrs).transaction(txnView).
                metricFactory(metricFactory);

            TransformingDDLDescriptor tdl = (TransformingDDLDescriptor) ddlChange.getTentativeDDLDesc();

            this.scanner = tdl.setScannerBuilderProperties(builder).build();
            this.runtimeContext = new SpliceRuntimeContext(ddlChange.getTxn());
        }
        return scanner;

    }

    private void transformResults(ExecRow row,
                                  RowTransformer transformer,
                                  CallBuffer<KVPair> writeBuffer) throws Exception {
        KVPair pair = transformer.transform(row);
        writeBuffer.add(pair);
    }

    private RecordingCallBuffer<KVPair> getWriteBuffer() {
        if (writeBuffer == null) {
            byte[] newTableLocation = Bytes.toBytes(Long.toString(ddlChange.getTentativeDDLDesc().getConglomerateNumber()));
            writeBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(newTableLocation, getTxn(),
                                                                             Metrics.noOpMetricFactory());
        }
        return writeBuffer;
    }

    private RowTransformer getTransformer() throws IOException {
        if (transformer == null) {
            transformer = ((TransformingDDLDescriptor) ddlChange.getTentativeDDLDesc()).createRowTransformer();
        }
        return transformer;
    }

    @Override
    public int getPriority() {
        return SchedulerPriorities.INSTANCE.getBasePriority(PopulateConglomerateTask.class);
    }

    @Override
    protected Txn beginChildTransaction(TxnView parentTxn, TxnLifecycleManager tc) throws IOException {
        return tc.beginChildTransaction(parentTxn, Long.toString(ddlChange.getTentativeDDLDesc().getConglomerateNumber()).getBytes());
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeInt(expectedScanReadWidth);
        out.writeLong(demarcationTimestamp);
        out.writeObject(ddlChange);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        expectedScanReadWidth = in.readInt();
        demarcationTimestamp = in.readLong();
        ddlChange = (DDLChange) in.readObject();
    }
}
