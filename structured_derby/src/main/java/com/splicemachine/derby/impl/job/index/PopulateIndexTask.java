package com.splicemachine.derby.impl.job.index;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
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
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.hbase.writer.RecordingCallBuffer;
import com.splicemachine.stats.MetricFactory;
import com.splicemachine.stats.Metrics;
import com.splicemachine.stats.TimeView;
import com.splicemachine.stats.Timer;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.concurrent.ExecutionException;

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
    private BitSet indexedColumns;
    private BitSet descColumns;

    private HRegion region;

    //performance improvement
    private KVPair mainPair;

		private String xplainSchema; //could be null, if no stats are to be collected
		private Timer writeTimer;

		public PopulateIndexTask() { }

    public PopulateIndexTask(String transactionId,
                             long indexConglomId,
                             long baseConglomId,
                             int[] mainColToIndexPosMap,
                             BitSet indexedColumns,
                             boolean unique,
                             String jobId,
                             BitSet descColumns,
														 String xplainSchema,
														 long statementId,
														 long operationId) {
        super(jobId, OperationJob.operationTaskPriority,transactionId,false);
        this.transactionId = transactionId;
        this.indexConglomId = indexConglomId;
        this.baseConglomId = baseConglomId;
        this.mainColToIndexPosMap = mainColToIndexPosMap;
        this.indexedColumns = indexedColumns;
        this.descColumns = descColumns;
        isUnique = unique;
				this.xplainSchema = xplainSchema;
				this.statementId = statementId;
				this.operationId = operationId;
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
        out.writeInt(descColumns.wlen);
        ArrayUtil.writeLongArray(out, descColumns.bits);
				out.writeBoolean(xplainSchema!=null);
				if(xplainSchema!=null){
						out.writeUTF(xplainSchema);
						out.writeLong(statementId);
						out.writeLong(operationId);
				}
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
        numWords = in.readInt();
        descColumns = new BitSet(ArrayUtil.readLongArray(in),numWords);
				if(in.readBoolean()){
						xplainSchema = in.readUTF();
						statementId = in.readLong();
						operationId = in.readLong();
				}
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
        regionScan.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY);
        //need to manually add the SIFilter, because it doesn't get added by region.getScanner(
        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(indexedColumns, ObjectArrayList.<Predicate>newInstance() ,true);
        regionScan.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,predicateFilter.toBytes());

				//TODO -sf- disable when stats tracking is disabled
				MetricFactory metricFactory = xplainSchema!=null? Metrics.basicMetricFactory(): Metrics.noOpMetricFactory();
				long numRecordsRead = 0l;
				long startTime = System.currentTimeMillis();

				writeTimer = metricFactory.newTimer();
				try{
						//backfill the index with previously committed data
						RegionScanner sourceScanner = region.getCoprocessorHost().preScannerOpen(regionScan);
						if(sourceScanner==null)
								sourceScanner = region.getScanner(regionScan);
						BufferedRegionScanner brs = new BufferedRegionScanner(region,sourceScanner,SpliceConstants.DEFAULT_CACHE_SIZE,metricFactory);
						try{
								List<KeyValue> nextRow = Lists.newArrayListWithExpectedSize(mainColToIndexPosMap.length);
								boolean shouldContinue = true;
								IndexTransformer transformer = IndexTransformer.newTransformer(indexedColumns,mainColToIndexPosMap,descColumns,isUnique);
								byte[] indexTableLocation = Bytes.toBytes(Long.toString(indexConglomId));
								RecordingCallBuffer<KVPair> writeBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(indexTableLocation,getTaskStatus().getTransactionId());
								try{
										while(shouldContinue){
												SpliceBaseOperation.checkInterrupt(numRecordsRead, SpliceConstants.interruptLoopCheck);
												nextRow.clear();
												shouldContinue  = brs.nextRaw(nextRow,null);
												numRecordsRead++;
												translateResult(nextRow, transformer,writeBuffer);
										}
								}finally{
										writeTimer.startTiming();
										writeBuffer.flushBuffer();
										writeBuffer.close();
										writeTimer.stopTiming(); //be sure and time the final flush wait time

										if(xplainSchema!=null){
												//record some stats
												OperationRuntimeStats stats = new OperationRuntimeStats(statementId,operationId,Bytes.toLong(taskId),12);
												stats.addMetric(OperationMetric.STOP_TIMESTAMP,System.currentTimeMillis());
												stats.addMetric(OperationMetric.START_TIMESTAMP,startTime);

												TimeView readTime = brs.getReadTime();
												stats.addMetric(OperationMetric.LOCAL_SCAN_BYTES,brs.getBytesRead());
												stats.addMetric(OperationMetric.LOCAL_SCAN_ROWS,brs.getRowsRead());
												stats.addMetric(OperationMetric.LOCAL_SCAN_WALL_TIME, readTime.getWallClockTime());
												stats.addMetric(OperationMetric.LOCAL_SCAN_CPU_TIME,readTime.getCpuTime());
												stats.addMetric(OperationMetric.LOCAL_SCAN_USER_TIME,readTime.getUserTime());

												TimeView writeTime = writeTimer.getTime();
												stats.addMetric(OperationMetric.WRITE_BYTES, writeBuffer.getTotalBytesAdded());
												stats.addMetric(OperationMetric.WRITE_ROWS, writeTimer.getNumEvents());
												stats.addMetric(OperationMetric.WRITE_WALL_TIME, writeTime.getWallClockTime());
												stats.addMetric(OperationMetric.WRITE_CPU_TIME,writeTime.getCpuTime());
												stats.addMetric(OperationMetric.WRITE_USER_TIME,writeTime.getUserTime());

												SpliceDriver.driver().getTaskReporter().report(xplainSchema,stats);
										}
//										if(LOG.isDebugEnabled()){
//												SpliceLogUtils.debug(LOG,"Total time to read %d records: %d ns",numRecordsRead,totalReadTime);
//												SpliceLogUtils.debug(LOG,"Average time to read 1 record: %f ns",(double)totalReadTime/numRecordsRead);
//												SpliceLogUtils.debug(LOG,"Total time to transform %d records: %d ns",numRecordsRead,totalTransformTime);
//												SpliceLogUtils.debug(LOG, "Average time to transform 1 record: %f ns", (double) totalTransformTime / numRecordsRead);
//												SpliceLogUtils.debug(LOG,"Total time to write %d records: %d ns",numRecordsRead,totalWriteTime);
//												SpliceLogUtils.debug(LOG,"Average time to write 1 record: %f ns",(double)totalWriteTime/numRecordsRead);
//										}
								}
						}finally{
								brs.close();
						}

        } catch (IOException e) {
            SpliceLogUtils.error(LOG, e);
            throw new ExecutionException(e);
        } catch (Exception e) {
            SpliceLogUtils.error(LOG, e);
            throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

    private void translateResult(List<KeyValue> result,
                                 IndexTransformer transformer,
                                 CallBuffer<KVPair> writeBuffer) throws Exception {
        //we know that there is only one KeyValue for each row
        for(KeyValue kv:result){
            //ignore SI CF
            if(kv.matchingFamily(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES)) continue;

            byte[] row = kv.getRow();
            byte[] data = kv.getValue();
            if(mainPair==null)
                mainPair = new KVPair();
            mainPair.setKey(row);
            mainPair.setValue(data);
            long start = System.nanoTime();
            KVPair pair = transformer.translate(mainPair);
            long end = System.nanoTime();

						writeTimer.startTiming();
            writeBuffer.add(pair);
						writeTimer.tick(1);
        }
    }

    @Override
    public int getPriority() {
        return SchedulerPriorities.INSTANCE.getBasePriority(PopulateIndexTask.class);
    }

}
