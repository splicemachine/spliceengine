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
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/5/13
 */
public class PopulateIndexTask extends ZkTask {
    private static final long serialVersionUID = 5l;
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
    private long totalTransformTime = 0l;
    private long totalWriteTime = 0l;

    public PopulateIndexTask() { }

    public PopulateIndexTask(String transactionId,
                             long indexConglomId,
                             long baseConglomId,
                             int[] mainColToIndexPosMap,
                             BitSet indexedColumns,
                             boolean unique,
                             String jobId,
                             BitSet descColumns) {
        super(jobId, OperationJob.operationTaskPriority,transactionId,false);
        this.transactionId = transactionId;
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

        long totalReadTime = 0l;
        long numRecordsRead = 0l;
        try{

            //backfill the index with previously committed data
            RegionScanner sourceScanner = region.getCoprocessorHost().preScannerOpen(regionScan);
            if(sourceScanner==null)
                sourceScanner = region.getScanner(regionScan);
            BufferedRegionScanner brs = new BufferedRegionScanner(region,sourceScanner,SpliceConstants.DEFAULT_CACHE_SIZE);
            try{
                List<KeyValue> nextRow = Lists.newArrayListWithExpectedSize(mainColToIndexPosMap.length);
                boolean shouldContinue = true;
                IndexTransformer transformer = IndexTransformer.newTransformer(indexedColumns,mainColToIndexPosMap,descColumns,isUnique);
                byte[] indexTableLocation = Bytes.toBytes(Long.toString(indexConglomId));
                CallBuffer<KVPair> writeBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(indexTableLocation,getTaskStatus().getTransactionId());
                try{
                    while(shouldContinue){
            			SpliceBaseOperation.checkInterrupt(numRecordsRead,SpliceConstants.interruptLoopCheck);
                        nextRow.clear();
                        long start = System.nanoTime();
                        shouldContinue  = brs.nextRaw(nextRow,null);
                        long stop = System.nanoTime();
                        totalReadTime+=(stop-start);
                        numRecordsRead++;
                        translateResult(nextRow, transformer,writeBuffer);
                    }
                }finally{
                    writeBuffer.flushBuffer();
                    writeBuffer.close();

                    if(LOG.isDebugEnabled()){
                        SpliceLogUtils.debug(LOG,"Total time to read %d records: %d ns",numRecordsRead,totalReadTime);
                        SpliceLogUtils.debug(LOG,"Average time to read 1 record: %f ns",(double)totalReadTime/numRecordsRead);
                        SpliceLogUtils.debug(LOG,"Total time to transform %d records: %d ns",numRecordsRead,totalTransformTime);
                        SpliceLogUtils.debug(LOG, "Average time to transform 1 record: %f ns", (double) totalTransformTime / numRecordsRead);
                        SpliceLogUtils.debug(LOG,"Total time to write %d records: %d ns",numRecordsRead,totalWriteTime);
                        SpliceLogUtils.debug(LOG,"Average time to write 1 record: %f ns",(double)totalWriteTime/numRecordsRead);
                    }
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
            totalTransformTime += (end-start);
            start = System.nanoTime();
            writeBuffer.add(pair);
            end = System.nanoTime();
            totalWriteTime+= (end-start);
        }
    }

    @Override
    public int getPriority() {
        return SchedulerPriorities.INSTANCE.getBasePriority(PopulateIndexTask.class);
    }
    public static void main (String [] args) {
        List<Integer> lista = new ArrayList<Integer>();
        while (true) {
            lista.add(new Integer(4834892));
        }
    }

}
