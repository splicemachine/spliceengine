package com.splicemachine.derby.impl.job.altertable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
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
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.DDLTxnView;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.si.impl.SIFilter;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.si.impl.TxnFilter;
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

    //performance improvement
    private Comparator<KeyValue> comparator;

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
    public void doExecute() throws ExecutionException, InterruptedException {
        long numRecordsRead = 0l;

        try {
            try (MeasuredRegionScanner scanner = getRegionScanner(Metrics.noOpMetricFactory())) {

                try {
                    // scanning the old table
                    List nextRow = Lists.newArrayListWithExpectedSize(expectedScanReadWidth);
                    boolean shouldContinue = true;
                    while (shouldContinue) {
                        SpliceBaseOperation.checkInterrupt(numRecordsRead, SpliceConstants.interruptLoopCheck);
                        nextRow.clear();
                        shouldContinue = scanner.internalNextRaw(nextRow);
                        if (shouldContinue) {
                            numRecordsRead++;
                            transformResults(nextRow, getTransformer(), getWriteBuffer());
                        }
                    }
                } finally {
                    if (writeBuffer != null) {
                        writeBuffer.flushBuffer();
                        writeBuffer.close();
                    }
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

    protected MeasuredRegionScanner getRegionScanner(MetricFactory metricFactory) throws IOException {
        Scan scan = SpliceUtils.createScan(getTxn());
        scan.setCaching(SpliceConstants.DEFAULT_CACHE_SIZE);
        scan.addFamily(SIConstants.DEFAULT_FAMILY_BYTES);
        scan.setStartRow(scanStart);
        scan.setStopRow(scanStop);
        scan.setCacheBlocks(false);
        scan.setMaxVersions();

        try(TransactionalRegion transactionalRegion = TransactionalRegions.get(region)){
            TxnFilter txnFilter = transactionalRegion.unpackedFilter(new DDLTxnView(getTxn(), this.demarcationTimestamp));
            scan.setFilter(new SIFilter(txnFilter));
        } // end try with resources

        RegionScanner regionScanner = region.getScanner(scan);

        return SpliceConstants.useReadAheadScanner ?
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
    }

    private void transformResults(List result,
                                  RowTransformer transformer,
                                  CallBuffer<KVPair> writeBuffer) throws Exception {
        // If result list has more than 1 member, there's more than one version
        // for the same row - sort by timestamp so they can be merged.
        List<KVPair> mergedResults = sortResults(result, dataLib, getResultComparitor());
        KVPair pair = transformer.transform(mergedResults);

        writeBuffer.add(pair);
    }

    private List<KVPair> sortResults(List<KeyValue> results, SDataLib dataLib, Comparator<KeyValue> comparitor) {
        Collections.sort(results, comparitor);
        List<KVPair> sortedResults = new ArrayList<>(results.size());
        for (KeyValue kv : results) {
            //ignore SI CF
            if (dataLib.getDataQualifierBuffer(kv)[dataLib.getDataQualifierOffset(kv)] != SIConstants.PACKED_COLUMN_BYTES[0])
                continue;
            byte[] row = dataLib.getDataRow(kv);
            byte[] data = dataLib.getDataValue(kv);
            sortedResults.add(new KVPair(row, data));
        }
        return sortedResults;
    }

    private Comparator<KeyValue> getResultComparitor() {
        if (comparator == null) {
            comparator = new TSComparator();
        }
        return comparator;
    }

    private static class TSComparator implements Comparator<KeyValue> {
        @Override
        public int compare(KeyValue o1, KeyValue o2) {
            if (o1 == null) {
                if (o2 == null) return 0;
                else return -1; // null ranked low
            }

            if (o2 == null) {
                return 1;   // null ranked low
            }

            long ts1 = o1.getTimestamp();
            long ts2 = o2.getTimestamp();
            if (ts1 < ts2) return -1;
            if (ts1 > ts2) return 1;
            return 0; // they're equal
        }
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
