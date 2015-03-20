package com.splicemachine.derby.impl.job.altertable;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.impl.sql.execute.altertable.AddColumnRowTransformer;
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
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.DDLTxnView;
import com.splicemachine.si.impl.HTransactorFactory;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.si.impl.SIFilter;
import com.splicemachine.si.impl.SIFilterPacked;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.si.impl.TxnFilter;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;

/**
 * @author Jeff Cunningham
 *         Date: 3/19/15
 */
public class PopulateConglomerateTask extends ZkTask {
    private static final long serialVersionUID = 1l;

    private String tableVersion;
    private long newConglomId;
    private long oldConglomId;
    private ColumnInfo[] newColumnInfos;
    private int[] columnOrdering;
    private long demarcationTimestamp;
    private static SDataLib dataLib = SIFactoryDriver.siFactory.getDataLib();

    //performance improvement
    private KVPair mainPair;

    private byte[] scanStart;
    private byte[] scanStop;

    public PopulateConglomerateTask() { }

    public PopulateConglomerateTask(String tableVersion,
                                    long newConglomId,
                                    long oldConglomId,
                                    String jobId,
                                    ColumnInfo[] newColumnInfos,
                                    int[] columnOrdering,
                                    long demarcationTimestamp) {
        super(jobId, OperationJob.operationTaskPriority,null);
        this.tableVersion = tableVersion;
        this.newConglomId = newConglomId;
        this.oldConglomId = oldConglomId;
        this.newColumnInfos = newColumnInfos;
        this.columnOrdering = columnOrdering;
        this.demarcationTimestamp = demarcationTimestamp;
    }

    @Override
    public RegionTask getClone() {
        return new PopulateConglomerateTask(tableVersion,newConglomId, oldConglomId,jobId,
                                            newColumnInfos,columnOrdering, demarcationTimestamp);
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
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(tableVersion);
        out.writeLong(newConglomId);
        out.writeLong(oldConglomId);
        out.writeInt(newColumnInfos.length);
        for (ColumnInfo col: newColumnInfos) {
            out.writeObject(col);
        }
        ArrayUtil.writeIntArray(out, columnOrdering);
        out.writeLong(demarcationTimestamp);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        tableVersion = (String) in.readObject();
        newConglomId = in.readLong();
        oldConglomId = in.readLong();
        int size = in.readInt();
        newColumnInfos = new ColumnInfo[size];
        for (int i = 0; i < size; ++i) {
            newColumnInfos[i] = (ColumnInfo)in.readObject();
        }
        columnOrdering = ArrayUtil.readIntArray(in);
        demarcationTimestamp = in.readLong();
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }


    @Override
    public void doExecute() throws ExecutionException, InterruptedException {
        long numRecordsRead = 0l;

        try {
            RecordingCallBuffer<KVPair> writeBuffer = null;
            try (MeasuredRegionScanner brs = getRegionScanner(Metrics.noOpMetricFactory())) {
                AddColumnRowTransformer transformer = AddColumnRowTransformer.create(tableVersion, columnOrdering,
                                                                                     newColumnInfos);

                byte[] newTableLocation = Bytes.toBytes(Long.toString(newConglomId));
                writeBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(newTableLocation, getTxn(),
                                                                                 Metrics.noOpMetricFactory());
                try {
                    // scanning the old table - one less column
                    List nextRow = Lists.newArrayListWithExpectedSize(newColumnInfos.length-1);
                    boolean shouldContinue = true;
                    while (shouldContinue) {
                        SpliceBaseOperation.checkInterrupt(numRecordsRead, SpliceConstants.interruptLoopCheck);
                        nextRow.clear();
                        shouldContinue = brs.internalNextRaw(nextRow);
                        numRecordsRead++;
                        transformResults(nextRow, transformer, writeBuffer);
                    }
                } finally {
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

    protected MeasuredRegionScanner getRegionScanner(MetricFactory metricFactory) throws IOException {
        Scan scan = SpliceUtils.createScan(getTxn());
        scan.setCaching(SpliceConstants.DEFAULT_CACHE_SIZE);
        scan.addFamily(SIConstants.DEFAULT_FAMILY_BYTES);
        scan.setStartRow(scanStart);
        scan.setStopRow(scanStop);
        scan.setCacheBlocks(false);

        TransactionalRegion transactionalRegion = TransactionalRegions.get(region);
        TxnFilter txnFilter = transactionalRegion.unpackedFilter(new DDLTxnView(getTxn(), this.demarcationTimestamp));
        transactionalRegion.discard();
        scan.setFilter(new SIFilter(txnFilter));

        RegionScanner regionScanner = region.getScanner(scan);

        return SpliceConstants.useReadAheadScanner ?
            new ReadAheadRegionScanner(region, SpliceConstants.DEFAULT_CACHE_SIZE, regionScanner,metricFactory, HTransactorFactory.getTransactor().getDataLib())
            : new BufferedRegionScanner(region,regionScanner,scan,SpliceConstants.DEFAULT_CACHE_SIZE,SpliceConstants.DEFAULT_CACHE_SIZE,metricFactory,HTransactorFactory.getTransactor().getDataLib());
    }

    private void transformResults(List result,
                                  AddColumnRowTransformer transformer,
                                  CallBuffer<KVPair> writeBuffer) throws Exception {
        //we know that there is only one KeyValue for each row
        for(Object kv:result){
            //ignore SI CF
            if (dataLib.getDataQualifierBuffer(kv)[dataLib.getDataQualifierOffset(kv)] != SIConstants.PACKED_COLUMN_BYTES[0])
                continue;
            byte[] row = dataLib.getDataRow(kv);
            byte[] data = dataLib.getDataValue(kv);
            if(mainPair==null)
                mainPair = new KVPair(row,data);
            else {
                mainPair.setKey(row);
                mainPair.setValue(data);
            }
            KVPair pair = transformer.transform(mainPair);

            writeBuffer.add(pair);
        }
    }

    @Override
    public int getPriority() {
        return SchedulerPriorities.INSTANCE.getBasePriority(PopulateConglomerateTask.class);
    }

    @Override
    protected Txn beginChildTransaction(TxnView parentTxn, TxnLifecycleManager tc) throws IOException {
        return tc.beginChildTransaction(parentTxn, Long.toString(this.newConglomId).getBytes());
    }
}
