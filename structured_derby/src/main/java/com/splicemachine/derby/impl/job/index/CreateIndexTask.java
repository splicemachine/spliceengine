package com.splicemachine.derby.impl.job.index;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.impl.sql.execute.LocalWriteContextFactory;
import com.splicemachine.derby.impl.sql.execute.index.IndexTransformer;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.storage.EntryPredicateFilter;
import com.splicemachine.storage.Predicate;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.MultiVersionConsistencyControl;
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
public class CreateIndexTask extends ZkTask {
    private static final long serialVersionUID = 5l;
    private long indexConglomId;
    private long baseConglomId;
    private int[] mainColToIndexPosMap;
    private boolean isUnique;
    private BitSet indexedColumns;
		private BitSet descColumns;

		private HRegion region;

		//performance improvement
    private KVPair mainPair;
    private long totalTransformTime = 0l;
    private long totalWriteTime = 0l;

    @SuppressWarnings("UnusedDeclaration")
		public CreateIndexTask() { }

    public CreateIndexTask(String transactionId,
                           long indexConglomId,
                           long baseConglomId,
                           int[] mainColToIndexPosMap,
                           BitSet indexedColumns,
                           boolean unique,
                           String jobId,
                           BitSet descColumns) {
        super(jobId, OperationJob.operationTaskPriority,transactionId,false);
        this.indexConglomId = indexConglomId;
        this.baseConglomId = baseConglomId;
        this.mainColToIndexPosMap = mainColToIndexPosMap;
        this.indexedColumns = indexedColumns;
        this.descColumns = descColumns;
        isUnique = unique;
    }

    @Override
    public void prepareTask(RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        this.region = rce.getRegion();
				super.prepareTask(rce, zooKeeper);
    }

    @Override
    protected String getTaskType() {
        return "createIndexTask";
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
        out.writeInt(descColumns.wlen);
        ArrayUtil.writeLongArray(out, descColumns.bits);        
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
        numWords = in.readInt();
        descColumns = new BitSet(ArrayUtil.readLongArray(in),numWords);
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }
    @Override
    public void doExecute() throws ExecutionException, InterruptedException {

        try{
            //add index to table watcher
            LocalWriteContextFactory contextFactory = SpliceIndexEndpoint.getContextFactory(baseConglomId);
            contextFactory.addIndex(indexConglomId, indexedColumns,mainColToIndexPosMap, isUnique,descColumns);

						//backfill the index with previously committed data
						RegionScanner sourceScanner = getScanner();
            region.startRegionOperation();
            MultiVersionConsistencyControl.setThreadReadPoint(sourceScanner.getMvccReadPoint());
            try{
								transformData(sourceScanner);
            }finally{
                region.closeRegionOperation();
                sourceScanner.close();
            }
        } catch (IOException e) {
        	SpliceLogUtils.error(LOG, e);
            throw new ExecutionException(e);
        } catch (Exception e) {
        	SpliceLogUtils.error(LOG, e);
            throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

		private RegionScanner getScanner() throws IOException {
				Scan regionScan = SpliceUtils.createScan(getTaskStatus().getTransactionId());
				regionScan.setCaching(SpliceConstants.DEFAULT_CACHE_SIZE);
				regionScan.setStartRow(HConstants.EMPTY_START_ROW);
				regionScan.setStopRow(HConstants.EMPTY_END_ROW);
				regionScan.setCacheBlocks(false);
				regionScan.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY);
				EntryPredicateFilter predicateFilter = new EntryPredicateFilter(indexedColumns, new ObjectArrayList<Predicate>(),true);
				regionScan.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,predicateFilter.toBytes());

				RegionScanner sourceScanner = region.getCoprocessorHost().preScannerOpen(regionScan);
				if(sourceScanner==null)
						sourceScanner = region.getScanner(regionScan);
				return sourceScanner;
		}

		private void transformData(RegionScanner sourceScanner) throws Exception {
				List<KeyValue> nextRow = Lists.newArrayListWithExpectedSize(mainColToIndexPosMap.length);
				IndexTransformer transformer = IndexTransformer.newTransformer(indexedColumns,mainColToIndexPosMap,descColumns,isUnique);
				byte[] indexTableLocation = Bytes.toBytes(Long.toString(indexConglomId));
				CallBuffer<KVPair> writeBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(indexTableLocation,getTaskStatus().getTransactionId());
				long totalReadTime =0l;
				long numRecordsRead =0l;
				try{
						boolean shouldContinue;
						do{
								nextRow.clear();
								long start = System.nanoTime();
								//bring in a block of records at once
								shouldContinue  = sourceScanner.nextRaw(nextRow,SpliceConstants.DEFAULT_CACHE_SIZE,null);
								long stop = System.nanoTime();
								totalReadTime+=(stop-start);
								numRecordsRead++;
								translateResult(nextRow, transformer,writeBuffer);
						}while(shouldContinue);
				}finally{
						writeBuffer.flushBuffer();
						writeBuffer.close();

						if(LOG.isInfoEnabled()){
								SpliceLogUtils.info(LOG, "Total time to read %d records: %d ns", numRecordsRead, totalReadTime);
								SpliceLogUtils.info(LOG,"Average time to read 1 record: %f ns",(double)totalReadTime/numRecordsRead);
								SpliceLogUtils.info(LOG,"Total time to transform %d records: %d ns",numRecordsRead,totalTransformTime);
								SpliceLogUtils.info(LOG, "Average time to transform 1 record: %f ns", (double) totalTransformTime / numRecordsRead);
								SpliceLogUtils.info(LOG,"Total time to write %d records: %d ns",numRecordsRead,totalWriteTime);
								SpliceLogUtils.info(LOG,"Average time to write 1 record: %f ns",(double)totalWriteTime/numRecordsRead);
						}
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
            totalTransformTime += (end-start);
            start = System.nanoTime();
            writeBuffer.add(pair);
            end = System.nanoTime();
            totalWriteTime+= (end-start);
        }
    }

		@Override
		public int getPriority() {
				return SchedulerPriorities.INSTANCE.getBasePriority(CreateIndexTask.class);
		}
}
