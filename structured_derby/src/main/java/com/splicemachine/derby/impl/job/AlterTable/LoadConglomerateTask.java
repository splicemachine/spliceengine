package com.splicemachine.derby.impl.job.AlterTable;


import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.impl.sql.execute.AlterTable.ConglomerateLoader;
import com.splicemachine.derby.impl.sql.execute.AlterTable.ConglomerateScanner;
import com.splicemachine.derby.impl.sql.execute.AlterTable.RowTransformer;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.impl.storage.KeyValueUtils;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.stats.MetricFactory;
import com.splicemachine.stats.Metrics;
import com.splicemachine.stats.TimeView;
import com.splicemachine.stats.Timer;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.derby.impl.sql.execute.ColumnInfo;
import org.apache.derby.catalog.UUID;
import org.apache.hadoop.hbase.util.Bytes;

import java.lang.Exception;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * Created with IntelliJ IDEA.
 * User: jyuan
 * Date: 2/6/14
 * Time: 10:36 AM
 * To change this template use File | Settings | File Templates.
 */
public class LoadConglomerateTask extends ZkTask {

    private UUID tableId;
    private long fromConglomId;
    private long toConglomId;
    private ColumnInfo[] columnInfo;
    private int droppedColumnPosition;
    private String txnId;
    private String xplainSchema;
    private long statementId;
    private long operationId;
    private ConglomerateLoader loader;
    private ConglomerateScanner scanner;
    private RowTransformer transformer;
    private KVPair newPair;
    private boolean initialized = false;
    private Timer writeTimer;

		private byte[] scanStart;
		private byte[] scanStop;

    public LoadConglomerateTask() {}

    public LoadConglomerateTask (UUID tableId,
                                 long fromConglomId,
                                 long toConglomId,
                                 ColumnInfo[] columnInfo,
                                 int droppedColumnPosition,
                                 String txnId,
                                 String jobId,
                                 String xplainSchema,
                                 long statementId,
                                 long operationId) {

        super(jobId, OperationJob.operationTaskPriority, txnId, false);
        this.tableId = tableId;
        this.fromConglomId = fromConglomId;
        this.toConglomId = toConglomId;
        this.columnInfo = columnInfo;
        this.droppedColumnPosition = droppedColumnPosition;
        this.txnId = txnId;
        this.xplainSchema = xplainSchema;
        this.statementId = statementId;
        this.operationId = operationId;
    }

		@Override
		public RegionTask getClone() {
				return new LoadConglomerateTask(tableId,
								fromConglomId,toConglomId,
								columnInfo,droppedColumnPosition,
								txnId,jobId,xplainSchema,statementId,operationId);
		}

		@Override public boolean isSplittable() { return false; }

		@Override
		public void prepareTask(byte[] start, byte[] stop, RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
				this.scanStart = start;
				this.scanStop = stop;
				super.prepareTask(start, stop, rce, zooKeeper);
		}

		private void initialize() throws StandardException{
        scanner = new ConglomerateScanner(columnInfo, region, txnId, xplainSchema,scanStart,scanStop);
        transformer = new RowTransformer(tableId, txnId, columnInfo, droppedColumnPosition);
        loader = new ConglomerateLoader(toConglomId, txnId);
        initialized = true;
    }
    @Override
    public void doExecute() throws ExecutionException, InterruptedException{
        long startTime = System.currentTimeMillis();
        long numRecordsRead = 0l;
        try {
            if (!initialized) {
                initialize();
            }
            MetricFactory metricFactory = xplainSchema!=null? Metrics.basicMetricFactory(): Metrics.noOpMetricFactory();
            writeTimer = metricFactory.newTimer();

            List<KeyValue> result = null;

            do {
                result = scanner.next();
                if (result == null) break;
                KeyValue kv = KeyValueUtils.matchDataColumn(result);
                SpliceBaseOperation.checkInterrupt(numRecordsRead, SpliceConstants.interruptLoopCheck);
                newPair = transformer.transform(kv);
                loader.add(newPair);
                numRecordsRead++;

            } while (result != null);
        }catch (Exception e) {
            throw new ExecutionException("Error loading conglomerate " + fromConglomId, e);
        } finally {
            writeTimer.startTiming();
            loader.close();
            writeTimer.stopTiming();
            if(xplainSchema!=null){
                //record some stats
                OperationRuntimeStats stats = new OperationRuntimeStats(statementId,operationId, Bytes.toLong(taskId),region.getRegionNameAsString(),12);
                stats.addMetric(OperationMetric.STOP_TIMESTAMP,System.currentTimeMillis());
                stats.addMetric(OperationMetric.START_TIMESTAMP,startTime);

                TimeView readTime = scanner.getReadTime();
                stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES,scanner.getBytesOutput());
                stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS,scanner.getRowsOutput());
                stats.addMetric(OperationMetric.LOCAL_SCAN_WALL_TIME, readTime.getWallClockTime());
                stats.addMetric(OperationMetric.LOCAL_SCAN_CPU_TIME,readTime.getCpuTime());
                stats.addMetric(OperationMetric.LOCAL_SCAN_USER_TIME,readTime.getUserTime());

                TimeView writeTime = writeTimer.getTime();
                stats.addMetric(OperationMetric.WRITE_BYTES, loader.getTotalBytesAdded());
                stats.addMetric(OperationMetric.WRITE_ROWS, writeTimer.getNumEvents());
                stats.addMetric(OperationMetric.PROCESSING_WALL_TIME, writeTime.getWallClockTime());
                stats.addMetric(OperationMetric.PROCESSING_CPU_TIME,writeTime.getCpuTime());
                stats.addMetric(OperationMetric.PROCESSING_USER_TIME,writeTime.getUserTime());

                SpliceDriver.driver().getTaskReporter().report(xplainSchema,stats);
            }
        }

    }

    @Override
    protected String getTaskType() {
        return "LoadConglomerateTask";
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }


		@Override
    public int getPriority() {
        return SchedulerPriorities.INSTANCE.getBasePriority(LoadConglomerateTask.class);
    }
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        tableId = (UUID) in.readObject();
        fromConglomId = in.readLong();
        toConglomId = in.readLong();
        int size = in.readInt();
        if (size > 0){
            columnInfo = new ColumnInfo[size];
            for (int i = 0; i < size; ++i){
                columnInfo[i] = (ColumnInfo)in.readObject();
            }
        }
        droppedColumnPosition = in.readInt();
        txnId = in.readUTF();

        if(in.readBoolean()){
            xplainSchema = in.readUTF();
            statementId = in.readLong();
            operationId = in.readLong();
        }
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(tableId);
        out.writeLong(fromConglomId);
        out.writeLong(toConglomId);
        int size = columnInfo.length;
        out.writeInt(size);
        for (int i = 0; i < size; ++i) {
            out.writeObject(columnInfo[i]);
        }
        out.writeInt(droppedColumnPosition);
        out.writeUTF(txnId);
        out.writeBoolean(xplainSchema!=null);

        if(xplainSchema!=null){
            out.writeUTF(xplainSchema);
            out.writeLong(statementId);
            out.writeLong(operationId);
        }
    }
}
