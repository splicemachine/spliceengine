package com.splicemachine.derby.impl.job.index;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.impl.sql.execute.index.IndexTransformer;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.metrics.OperationMetric;
import com.splicemachine.derby.metrics.OperationRuntimeStats;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.hbase.ReadAheadRegionScanner;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.metrics.Timer;
import com.splicemachine.si.api.TransactionalRegion;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnLifecycleManager;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.DDLTxnView;
import com.splicemachine.si.impl.SIFilterPacked;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.si.impl.TxnFilter;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.pipeline.api.RecordingCallBuffer;
import com.splicemachine.pipeline.api.WriteStats;
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
    private long indexConglomId;
    private long baseConglomId;
    private int[] mainColToIndexPosMap;
    private boolean isUnique;
    private boolean isUniqueWithDuplicateNulls;
    private BitSet indexedColumns;
    private BitSet descColumns;
    private int[] columnOrdering;
    private int[] format_ids;
    private long demarcationPoint;

    private HRegion region;

    //performance improvement
    private KVPair mainPair;

		private byte[] scanStart;
		private byte[] scanStop;
        boolean isTraced; //could be null, if no stats are to be collected

		@SuppressWarnings("UnusedDeclaration")
		public PopulateIndexTask() { }

    public PopulateIndexTask(
                             long indexConglomId,
                             long baseConglomId,
                             int[] mainColToIndexPosMap,
                             BitSet indexedColumns,
                             boolean unique,
                             boolean uniqueWithDuplicateNulls,
                             String jobId,
                             BitSet descColumns,
                             boolean isTraced,
                             long statementId,
                             long operationId,
                             int[] columnOrdering,
                             int[] format_ids,
                             long demarcationPoint) {
        super(jobId, OperationJob.operationTaskPriority,null);
        this.indexConglomId = indexConglomId;
        this.baseConglomId = baseConglomId;
        this.mainColToIndexPosMap = mainColToIndexPosMap;
        this.indexedColumns = indexedColumns;
        this.descColumns = descColumns;
        this.isUnique = unique;
        this.isUniqueWithDuplicateNulls = uniqueWithDuplicateNulls;
        this.isTraced = isTraced;
        this.statementId = statementId;
        this.operationId = operationId;
        this.columnOrdering = columnOrdering;
        this.format_ids = format_ids;
        this.demarcationPoint = demarcationPoint;
    }

		@Override
		public RegionTask getClone() {
				return new PopulateIndexTask(indexConglomId,baseConglomId,mainColToIndexPosMap,indexedColumns,isUnique,
								isUniqueWithDuplicateNulls,jobId,descColumns,isTraced,statementId,operationId,columnOrdering,format_ids,demarcationPoint);
		}

		@Override
		public boolean isSplittable() {
				return true;
		}

		@Override
    public void prepareTask(byte[] start, byte[] end,RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        this.region = rce.getRegion();
        super.prepareTask(start,end,rce, zooKeeper);
				this.scanStart = start;
				this.scanStop = end;
    }

    @Override
    protected String getTaskType() {
        return "populateIndexTask";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeLong(indexConglomId);
        out.writeLong(baseConglomId);
        out.writeInt(indexedColumns.wlen);
        ArrayUtil.writeLongArray(out, indexedColumns.bits);
        ArrayUtil.writeIntArray(out, mainColToIndexPosMap);
        out.writeBoolean(isUnique);
        out.writeBoolean(isUniqueWithDuplicateNulls);
        out.writeInt(descColumns.wlen);
        ArrayUtil.writeLongArray(out, descColumns.bits);
        out.writeBoolean(isTraced);
        if(isTraced){
            out.writeLong(statementId);
            out.writeLong(operationId);
        }
        ArrayUtil.writeIntArray(out, columnOrdering);
        ArrayUtil.writeIntArray(out, format_ids);
        out.writeLong(demarcationPoint);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        indexConglomId = in.readLong();
        baseConglomId = in.readLong();
        int numWords = in.readInt();
        indexedColumns = new BitSet(ArrayUtil.readLongArray(in),numWords);
        mainColToIndexPosMap = ArrayUtil.readIntArray(in);
        isUnique = in.readBoolean();
        isUniqueWithDuplicateNulls = in.readBoolean();
        numWords = in.readInt();
        descColumns = new BitSet(ArrayUtil.readLongArray(in),numWords);
        if(isTraced = in.readBoolean()){
            statementId = in.readLong();
            operationId = in.readLong();
        }
        columnOrdering = ArrayUtil.readIntArray(in);
        format_ids = ArrayUtil.readIntArray(in);
        demarcationPoint = in.readLong();
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }


		@Override
    public void doExecute() throws ExecutionException, InterruptedException {
        Scan regionScan = SpliceUtils.createScan(getTxn());
        regionScan.setCaching(SpliceConstants.DEFAULT_CACHE_SIZE);
        regionScan.setStartRow(scanStart);
        regionScan.setStopRow(scanStop);
        regionScan.setCacheBlocks(false);

				//TODO -sf- disable when stats tracking is disabled
				MetricFactory metricFactory = isTraced? Metrics.samplingMetricFactory(SpliceConstants.sampleTimingSize): Metrics.noOpMetricFactory();
				long numRecordsRead = 0l;
				long startTime = System.currentTimeMillis();

				Timer transformationTimer = metricFactory.newTimer();
				try{
						//backfill the index with previously committed data

            EntryPredicateFilter predicateFilter = new EntryPredicateFilter(indexedColumns, ObjectArrayList.<Predicate>newInstance() ,true);
            MeasuredRegionScanner brs = getRegionScanner(predicateFilter,regionScan, metricFactory);

            RecordingCallBuffer<KVPair> writeBuffer = null;
						try{
								List<KeyValue> nextRow = Lists.newArrayListWithExpectedSize(mainColToIndexPosMap.length);
								boolean shouldContinue = true;
								boolean[] ascDescInfo = new boolean[format_ids.length];
								Arrays.fill(ascDescInfo,true);
								for(int i=descColumns.nextSetBit(0);i>=0;i=descColumns.nextSetBit(i+1))
										ascDescInfo[i] = false;

								int[] keyEncodingMap = new int[format_ids.length];
								Arrays.fill(keyEncodingMap,-1);
								for(int i=indexedColumns.nextSetBit(0);i>=0;i=indexedColumns.nextSetBit(i+1)){
										keyEncodingMap[i] = mainColToIndexPosMap[i];
								}
								IndexTransformer transformer = new IndexTransformer(isUnique,isUniqueWithDuplicateNulls,null,
												columnOrdering,
												format_ids,
												null,
												keyEncodingMap,
												ascDescInfo);

								byte[] indexTableLocation = Bytes.toBytes(Long.toString(indexConglomId));
								writeBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(indexTableLocation,getTxn(),metricFactory);
								try{
										while(shouldContinue){
												SpliceBaseOperation.checkInterrupt(numRecordsRead, SpliceConstants.interruptLoopCheck);
												nextRow.clear();
												shouldContinue  = brs.nextRaw(nextRow,null);
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

    protected MeasuredRegionScanner getRegionScanner(EntryPredicateFilter predicateFilter,Scan regionScan, MetricFactory metricFactory) throws IOException {
        //manually create the SIFilter
        DDLTxnView demarcationPoint = new DDLTxnView(getTxn(), this.demarcationPoint);
        TransactionalRegion transactionalRegion = TransactionalRegions.get(region);
        TxnFilter packed = transactionalRegion.packedFilter(demarcationPoint, predicateFilter, false);
        transactionalRegion.discard();
        regionScan.setFilter(new SIFilterPacked(packed));
        RegionScanner sourceScanner = region.getScanner(regionScan);
        return SpliceConstants.useReadAheadScanner? new ReadAheadRegionScanner(region, SpliceConstants.DEFAULT_CACHE_SIZE, sourceScanner,metricFactory)
                        : new BufferedRegionScanner(region,sourceScanner,regionScan,SpliceConstants.DEFAULT_CACHE_SIZE,SpliceConstants.DEFAULT_CACHE_SIZE,metricFactory);
    }

    protected void reportStats(long startTime, MeasuredRegionScanner brs, RecordingCallBuffer<KVPair> writeBuffer,TimeView manipulationTime) throws IOException {
				if(isTraced){
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

						SpliceDriver.driver().getTaskReporter().report(stats,getTxn());
				}
		}

		private void translateResult(List<KeyValue> result,
                                 IndexTransformer transformer,
                                 CallBuffer<KVPair> writeBuffer,
																 Timer manipulationTimer) throws Exception {
        //we know that there is only one KeyValue for each row
				manipulationTimer.startTiming();
        for(KeyValue kv:result){
            //ignore SI CF
        	if (kv.getBuffer()[kv.getQualifierOffset()] != SIConstants.PACKED_COLUMN_BYTES[0])
        		continue;
            byte[] row = kv.getRow();
            byte[] data = kv.getValue();
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

    @Override
    protected Txn beginChildTransaction(TxnView parentTxn, TxnLifecycleManager tc) throws IOException {
        return tc.beginChildTransaction(parentTxn, Long.toString(this.indexConglomId).getBytes());
    }
}
