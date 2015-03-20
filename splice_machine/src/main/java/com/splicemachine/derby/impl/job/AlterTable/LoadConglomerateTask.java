package com.splicemachine.derby.impl.job.altertable;


import com.google.common.io.Closeables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.impl.sql.execute.altertable.ConglomerateLoader;
import com.splicemachine.derby.impl.sql.execute.altertable.ConglomerateScanner;
import com.splicemachine.derby.impl.sql.execute.altertable.DropColumnRowTransformer;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.pipeline.api.WriteStats;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.utils.SpliceZooKeeperManager;

import com.splicemachine.db.iapi.error.StandardException;

import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import com.splicemachine.db.impl.sql.execute.ColumnInfo;
import com.splicemachine.db.catalog.UUID;
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
    private boolean isTraced;
    private long statementId;
    private long operationId;
    private ConglomerateLoader loader;
    private ConglomerateScanner scanner;
    private DropColumnRowTransformer transformer;
    private KVPair newPair;
    private boolean initialized = false;
    private Timer writeTimer;
    private long demarcationPoint;

		private byte[] scanStart;
		private byte[] scanStop;

    public LoadConglomerateTask() {}

    public LoadConglomerateTask (UUID tableId,
                                 long fromConglomId,
                                 long toConglomId,
                                 ColumnInfo[] columnInfo,
                                 int droppedColumnPosition,
                                 String jobId,
                                 boolean isTraced,
                                 long statementId,
                                 long operationId,
                                 long demarcationPoint) {

        super(jobId, OperationJob.operationTaskPriority, null);
        this.tableId = tableId;
        this.fromConglomId = fromConglomId;
        this.toConglomId = toConglomId;
        this.columnInfo = columnInfo;
        this.droppedColumnPosition = droppedColumnPosition;
        this.isTraced = isTraced;
        this.statementId = statementId;
        this.operationId = operationId;
        this.demarcationPoint = demarcationPoint;
    }

		@Override
		public RegionTask getClone() {
				return new LoadConglomerateTask(tableId,
								fromConglomId,toConglomId,
								columnInfo,droppedColumnPosition,
				jobId,isTraced,statementId,operationId,demarcationPoint);
		}

		@Override public boolean isSplittable() { return false; }

		@Override
		public void prepareTask(byte[] start, byte[] stop, RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
				this.scanStart = start;
				this.scanStop = stop;
				super.prepareTask(start, stop, rce, zooKeeper);
		}

		private void initialize() throws StandardException{
        Txn txn = getTxn();
        scanner = new ConglomerateScanner(region, txn,demarcationPoint, isTraced,scanStart,scanStop);
        transformer = new DropColumnRowTransformer(tableId, txn, columnInfo, droppedColumnPosition);
        loader = new ConglomerateLoader(toConglomId, txn, isTraced);
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
            MetricFactory metricFactory = isTraced? Metrics.basicMetricFactory(): Metrics.noOpMetricFactory();
            writeTimer = metricFactory.newTimer();

            List result;
            writeTimer.startTiming();
            do {
                SpliceBaseOperation.checkInterrupt(numRecordsRead, SpliceConstants.interruptLoopCheck);
                result = scanner.next();
                if (result == null) continue;

                newPair = transformer.transform(dataLib.matchDataColumn(result));
                loader.add(newPair);
                numRecordsRead++;

            } while (result != null);
        }catch (Exception e) {
            throw new ExecutionException("Error loading conglomerate " + fromConglomId, e);
        } finally {
            WriteStats writeStats = loader.getWriteStats();
			Closeables.closeQuietly(transformer);
            loader.close();
            writeTimer.stopTiming();
            if(isTraced){
                //record some stats
                OperationRuntimeStats stats = new OperationRuntimeStats(statementId,operationId, Bytes.toLong(taskId),region.getRegionNameAsString(),12);
                stats.addMetric(OperationMetric.STOP_TIMESTAMP,System.currentTimeMillis());
                stats.addMetric(OperationMetric.START_TIMESTAMP, startTime);
                stats.addMetric(OperationMetric.TOTAL_WALL_TIME, writeTimer.getTime().getWallClockTime());

                TimeView readTime = scanner.getReadTime();
                stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES,scanner.getBytesOutput());
                stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS,scanner.getRowsOutput());
                stats.addMetric(OperationMetric.LOCAL_SCAN_WALL_TIME, readTime.getWallClockTime());
                stats.addMetric(OperationMetric.LOCAL_SCAN_CPU_TIME,readTime.getCpuTime());
                stats.addMetric(OperationMetric.LOCAL_SCAN_USER_TIME,readTime.getUserTime());


                TimeView time = writeStats.getTotalTime();
                stats.addMetric(OperationMetric.WRITE_BYTES, writeStats.getBytesWritten());
                stats.addMetric(OperationMetric.WRITE_ROWS, writeStats.getRowsWritten());
                stats.addMetric(OperationMetric.WRITE_TOTAL_WALL_TIME, time.getWallClockTime());

                try{
                    SpliceDriver.driver().getTaskReporter().report(stats,getTxn());
                }catch(IOException ioe){
                    throw new ExecutionException(ioe);
                }
            }
        }

    }

    @Override
    protected Txn beginChildTransaction(TxnView parentTxn, TxnLifecycleManager tc) throws IOException {
        return tc.beginChildTransaction(parentTxn,Long.toString(toConglomId).getBytes());
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

        if(isTraced = in.readBoolean()){
            statementId = in.readLong();
            operationId = in.readLong();
        }
        demarcationPoint = in.readLong();
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
        out.writeBoolean(isTraced);

        if(isTraced){
            out.writeLong(statementId);
            out.writeLong(operationId);
        }
        out.writeLong(demarcationPoint);
    }
}
