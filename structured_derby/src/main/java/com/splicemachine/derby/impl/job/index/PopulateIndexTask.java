package com.splicemachine.derby.impl.job.index;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.concurrent.ExecutionException;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.impl.sql.execute.index.IndexTransformer;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.RecordingCallBuffer;
import com.splicemachine.hbase.writer.WriteStats;
import com.splicemachine.stats.MetricFactory;
import com.splicemachine.stats.Metrics;
import com.splicemachine.stats.TimeView;
import com.splicemachine.stats.Timer;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;

/**
 * @author Scott Fines
 * Created on: 4/5/13
 */
public class PopulateIndexTask extends ZkTask {
    private static final long serialVersionUID = 5l;
		private long operationId;
		private long statementId;
		private String transactionId;
    private long indexConglomId;
    private long baseConglomId;
    private int[] mainColToIndexPosMap;
    private boolean isUnique;
    private boolean isUniqueWithDuplicateNulls;
    private BitSet indexedColumns;
    private BitSet descColumns;
    private int[] columnOrdering;
    private int[] format_ids;

    private HRegion region;

    //performance improvement
    private KVPair mainPair;

		private String xplainSchema; //could be null, if no stats are to be collected

		@SuppressWarnings("UnusedDeclaration")
		public PopulateIndexTask() { }

    public PopulateIndexTask(String transactionId,
                             long indexConglomId,
                             long baseConglomId,
                             int[] mainColToIndexPosMap,
                             BitSet indexedColumns,
                             boolean unique,
                             boolean uniqueWithDuplicateNulls,
                             String jobId,
                             BitSet descColumns,
                             String xplainSchema,
                             long statementId,
                             long operationId,
                             int[] columnOrdering,
                             int[] format_ids) {
        super(jobId, OperationJob.operationTaskPriority,transactionId,false);
        this.transactionId = transactionId;
        this.indexConglomId = indexConglomId;
        this.baseConglomId = baseConglomId;
        this.mainColToIndexPosMap = mainColToIndexPosMap;
        this.indexedColumns = indexedColumns;
        this.descColumns = descColumns;
        this.isUnique = unique;
        this.isUniqueWithDuplicateNulls = uniqueWithDuplicateNulls;
        this.xplainSchema = xplainSchema;
        this.statementId = statementId;
        this.operationId = operationId;
        this.columnOrdering = columnOrdering;
        this.format_ids = format_ids;
    }

    @Override
    public void prepareTask(RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        this.region = rce.getRegion();
        super.prepareTask(rce, zooKeeper);
    }

    @Override
    protected String getTaskType() {
        return "populateIndexTask";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeUTF(transactionId);
        out.writeLong(indexConglomId);
        out.writeLong(baseConglomId);
        out.writeInt(indexedColumns.wlen);
        ArrayUtil.writeLongArray(out, indexedColumns.bits);
        ArrayUtil.writeIntArray(out, mainColToIndexPosMap);
        out.writeBoolean(isUnique);
        out.writeBoolean(isUniqueWithDuplicateNulls);
        out.writeInt(descColumns.wlen);
        ArrayUtil.writeLongArray(out, descColumns.bits);
        out.writeBoolean(xplainSchema!=null);
        if(xplainSchema!=null){
            out.writeUTF(xplainSchema);
            out.writeLong(statementId);
            out.writeLong(operationId);
        }
        ArrayUtil.writeIntArray(out, columnOrdering);
        ArrayUtil.writeIntArray(out, format_ids);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        transactionId = in.readUTF();
        indexConglomId = in.readLong();
        baseConglomId = in.readLong();
        int numWords = in.readInt();
        indexedColumns = new BitSet(ArrayUtil.readLongArray(in),numWords);
        mainColToIndexPosMap = ArrayUtil.readIntArray(in);
        isUnique = in.readBoolean();
        isUniqueWithDuplicateNulls = in.readBoolean();
        numWords = in.readInt();
        descColumns = new BitSet(ArrayUtil.readLongArray(in),numWords);
        if(in.readBoolean()){
            xplainSchema = in.readUTF();
            statementId = in.readLong();
            operationId = in.readLong();
        }
        columnOrdering = ArrayUtil.readIntArray(in);
        format_ids = ArrayUtil.readIntArray(in);
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }
    @Override
    public void doExecute() throws ExecutionException, InterruptedException {
        Scan regionScan = SpliceUtils.createScan(transactionId);
        regionScan.setCaching(SpliceConstants.DEFAULT_CACHE_SIZE);
        regionScan.setStartRow(region.getStartKey());
        regionScan.setStopRow(region.getEndKey());
        regionScan.setCacheBlocks(false);
        regionScan.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY);
        //need to manually add the SIFilter, because it doesn't get added by region.getScanner(
        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(indexedColumns, ObjectArrayList.<Predicate>newInstance() ,true);
        regionScan.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,predicateFilter.toBytes());

				//TODO -sf- disable when stats tracking is disabled
                MetricFactory metricFactory = xplainSchema!=null? Metrics.samplingMetricFactory(SpliceConstants.sampleTimingSize): Metrics.noOpMetricFactory();
                long numRecordsRead = 0l;
				long startTime = System.currentTimeMillis();

                Timer transformationTimer = metricFactory.newTimer();
				try{
						//backfill the index with previously committed data
						RegionScanner sourceScanner = region.getCoprocessorHost().preScannerOpen(regionScan);
						if(sourceScanner==null)
								sourceScanner = region.getScanner(regionScan);
						BufferedRegionScanner brs = new BufferedRegionScanner(region,sourceScanner,regionScan,SpliceConstants.DEFAULT_CACHE_SIZE,metricFactory);
                        RecordingCallBuffer<KVPair> writeBuffer = null;
                        try{
								List<Cell> nextRow = Lists.newArrayListWithExpectedSize(mainColToIndexPosMap.length);
								boolean shouldContinue = true;
								IndexTransformer transformer =
                                        IndexTransformer.newTransformer(indexedColumns,mainColToIndexPosMap,descColumns,
                                                isUnique,isUniqueWithDuplicateNulls,columnOrdering,format_ids);

								byte[] indexTableLocation = Bytes.toBytes(Long.toString(indexConglomId));
								writeBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(indexTableLocation,getTaskStatus().getTransactionId(),metricFactory);
								try{
										while(shouldContinue){
												SpliceBaseOperation.checkInterrupt(numRecordsRead, SpliceConstants.interruptLoopCheck);
												nextRow.clear();
												shouldContinue  = brs.nextRaw(nextRow);
												numRecordsRead++;
												translateResult(nextRow, transformer, writeBuffer, transformationTimer);
										}
								}finally{
										transformationTimer.startTiming();
										writeBuffer.flushBuffer();
										writeBuffer.close();
										transformationTimer.stopTiming();
								}
						}finally{
								brs.close();
						}

						reportStats(startTime, brs, writeBuffer, transformationTimer.getTime());

        } catch (IOException e) {
            SpliceLogUtils.error(LOG, e);
            throw new ExecutionException(e);
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, e);
            throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

		protected void reportStats(long startTime, BufferedRegionScanner brs, RecordingCallBuffer<KVPair> writeBuffer,TimeView manipulationTime) {
				if(xplainSchema!=null){
						//record some stats
						OperationRuntimeStats stats = new OperationRuntimeStats(statementId,operationId, Bytes.toLong(taskId),region.getRegionNameAsString(),12);
						stats.addMetric(OperationMetric.STOP_TIMESTAMP,System.currentTimeMillis());

						WriteStats writeStats = writeBuffer.getWriteStats();
						TimeView readTime = brs.getReadTime();
						stats.addMetric(OperationMetric.START_TIMESTAMP,startTime);
						stats.addMetric(OperationMetric.TASK_QUEUE_WAIT_WALL_TIME,waitTimeNs);
						stats.addMetric(OperationMetric.OUTPUT_ROWS,writeStats.getRowsWritten());
						stats.addMetric(OperationMetric.TOTAL_WALL_TIME, manipulationTime.getWallClockTime()+readTime.getWallClockTime());
						stats.addMetric(OperationMetric.TOTAL_CPU_TIME, manipulationTime.getCpuTime()+readTime.getCpuTime());
						stats.addMetric(OperationMetric.TOTAL_USER_TIME,manipulationTime.getUserTime()+readTime.getUserTime());

						stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES,brs.getBytesVisited());
						stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS,brs.getRowsVisited());
						stats.addMetric(OperationMetric.LOCAL_SCAN_WALL_TIME, readTime.getWallClockTime());
						stats.addMetric(OperationMetric.LOCAL_SCAN_CPU_TIME,readTime.getCpuTime());
						stats.addMetric(OperationMetric.LOCAL_SCAN_USER_TIME,readTime.getUserTime());

						stats.addMetric(OperationMetric.PROCESSING_WALL_TIME, manipulationTime.getWallClockTime());
						stats.addMetric(OperationMetric.PROCESSING_CPU_TIME,manipulationTime.getCpuTime());
						stats.addMetric(OperationMetric.PROCESSING_USER_TIME,manipulationTime.getUserTime());

						OperationRuntimeStats.addWriteStats(writeStats,stats);

						SpliceDriver.driver().getTaskReporter().report(xplainSchema,stats);
				}
		}

		private void translateResult(List<Cell> result,
                                 IndexTransformer transformer,
                                 CallBuffer<KVPair> writeBuffer,
																 Timer manipulationTimer) throws Exception {
        //we know that there is only one KeyValue for each row
				manipulationTimer.startTiming();
        for(Cell kv:result){
            //ignore SI CF
						if(!CellUtils.singleMatchingQualifier(kv, SIConstants.PACKED_COLUMN_BYTES))
								continue;
						byte[] row = CellUtil.cloneRow(kv);
						byte[] data = CellUtil.cloneValue(kv);
						if(mainPair==null)
								mainPair = new KVPair();
						mainPair.setKey(row);
						mainPair.setValue(data);
						KVPair pair = transformer.translate(mainPair);

            writeBuffer.add(pair);
        }
				manipulationTimer.tick(1);
    }

    @Override
    public int getPriority() {
        return SchedulerPriorities.INSTANCE.getBasePriority(PopulateIndexTask.class);
    }

}
