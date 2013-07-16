package com.splicemachine.derby.impl.job.index;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.sql.execute.index.WriteContextFactoryPool;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.*;
import com.splicemachine.hbase.batch.WriteContextFactory;
import com.splicemachine.storage.*;
import com.splicemachine.storage.index.BitIndex;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.nio.ByteBuffer;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/5/13
 */
public class CreateIndexTask extends ZkTask {
    private static final long serialVersionUID = 3l;
    private String transactionId;
    private long indexConglomId;
    private long baseConglomId;
    private int[] mainColToIndexPosMap;
    private boolean isUnique;
    private BitSet indexedColumns;
    private BitSet nonUniqueIndexColumns;

    private MultiFieldEncoder translateEncoder;

    private HRegion region;

    public CreateIndexTask() { }

    public CreateIndexTask(String transactionId,
                           long indexConglomId,
                           long baseConglomId,
                           int[] mainColToIndexPosMap,
                           BitSet indexedColumns,
                           boolean unique,
                           String jobId ) {
        super(jobId, OperationJob.operationTaskPriority,transactionId,false);
        this.transactionId = transactionId;
        this.indexConglomId = indexConglomId;
        this.baseConglomId = baseConglomId;
        this.mainColToIndexPosMap = mainColToIndexPosMap;
        this.indexedColumns = indexedColumns;
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
        out.writeUTF(transactionId);
        out.writeLong(indexConglomId);
        out.writeLong(baseConglomId);
        out.writeObject(indexedColumns);
        ArrayUtil.writeIntArray(out, mainColToIndexPosMap);
        out.writeBoolean(isUnique);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        transactionId = in.readUTF();
        indexConglomId = in.readLong();
        baseConglomId = in.readLong();
        indexedColumns = (BitSet)in.readObject();
        mainColToIndexPosMap = ArrayUtil.readIntArray(in);
        isUnique = in.readBoolean();
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }

    @Override
    public void execute() throws ExecutionException, InterruptedException {
        Scan regionScan = SpliceUtils.createScan(transactionId);
        regionScan.setCaching(DEFAULT_CACHE_SIZE);
        regionScan.setStartRow(region.getStartKey());
        regionScan.setStopRow(region.getEndKey());
        regionScan.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES, RowMarshaller.PACKED_COLUMN_KEY);
        //need to manually add the SIFilter, because it doesn't get added by region.getScanner(
        EntryPredicateFilter predicateFilter = new EntryPredicateFilter(indexedColumns, Collections.<Predicate>emptyList(),true);
        regionScan.setAttribute(SpliceConstants.ENTRY_PREDICATE_LABEL,predicateFilter.toBytes());

        nonUniqueIndexColumns = (BitSet)indexedColumns.clone();
        nonUniqueIndexColumns.set(indexedColumns.length());

        try{
            //add index to table watcher
            WriteContextFactory contextFactory = WriteContextFactoryPool.getContextFactory(baseConglomId);
            contextFactory.addIndex(indexConglomId, indexedColumns,mainColToIndexPosMap, isUnique);
            
            //backfill the index with previously committed data
            RegionScanner sourceScanner = region.getCoprocessorHost().preScannerOpen(regionScan);
            if(sourceScanner==null)
                sourceScanner = region.getScanner(regionScan);
            byte[] indexBytes = Long.toString(indexConglomId).getBytes();
            CallBuffer<Mutation> writeBuffer =
                    SpliceDriver.driver().getTableWriter().writeBuffer(indexBytes, new TableWriter.FlushWatcher() {
                        @Override
                        public List<Mutation> preFlush(List<Mutation> mutations) throws Exception {
                            return mutations;
                        }

                        @Override
                        public Response globalError(Throwable t) throws Exception {
                            if(t instanceof NotServingRegionException) return Response.RETRY;
                            else if(t instanceof WrongRegionException) return Response.RETRY;
                            else
                                return Response.THROW_ERROR;
                        }

                        @Override
                        public Response partialFailure(MutationRequest request, MutationResponse response) throws Exception {
                            for(MutationResult result : response.getFailedRows().values()){
                                if(result.isRetryable())
                                    return Response.RETRY;
                            }
                            return  Response.THROW_ERROR;
                        }
                    });

            List < KeyValue > nextRow = Lists.newArrayListWithExpectedSize(mainColToIndexPosMap.length);
            boolean shouldContinue = true;
            while(shouldContinue){
                nextRow.clear();
                shouldContinue  = sourceScanner.next(nextRow);
                List<Put> indexPuts = translateResult(nextRow);

                writeBuffer.addAll(indexPuts);
            }
            writeBuffer.flushBuffer();
            writeBuffer.close();

        } catch (IOException e) {
            throw new ExecutionException(e);
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    private List<Put> translateResult(List<KeyValue> result) throws IOException{
        EntryAccumulator keyAccumulator;
        if(isUnique)
            keyAccumulator = new SparseEntryAccumulator(null,indexedColumns,false);
        else
            keyAccumulator = new SparseEntryAccumulator(null,nonUniqueIndexColumns,false);

        EntryAccumulator rowAccumulator = new SparseEntryAccumulator(null,nonUniqueIndexColumns,true);


        EntryDecoder decoder = new EntryDecoder();
        //we know that there is only one KeyValue for each row
        List<Put> indexPuts = Lists.newArrayListWithExpectedSize(result.size());
        for(KeyValue kv:result){
            if(Bytes.equals(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES,kv.getFamily()))
                continue; //ignore SI keyValues
            keyAccumulator.reset();
            rowAccumulator.reset();

            decoder.set(kv.getValue());

            BitIndex mainTableIndex = decoder.getCurrentIndex();

            MultiFieldDecoder fieldDecoder = decoder.getEntryDecoder();
            for(int mainTableCol=mainTableIndex.nextSetBit(0);
                mainTableCol>=0&&mainTableCol<=indexedColumns.length();
                mainTableCol=mainTableIndex.nextSetBit(mainTableCol+1)){

                if(indexedColumns.get(mainTableCol)){
                    ByteBuffer buffer = decoder.nextAsBuffer(fieldDecoder,mainTableCol);
                    accumulate(keyAccumulator,mainTableIndex,buffer,mainTableCol);
                    accumulate(rowAccumulator,mainTableIndex,buffer,mainTableCol);
                }
            }

            if(!isUnique){
                keyAccumulator.add(indexedColumns.length(),ByteBuffer.wrap(SpliceUtils.getUniqueKey()));
            }
            byte[] finalIndexRow = keyAccumulator.finish();
            Put put = SpliceUtils.createPut(finalIndexRow,transactionId);

            rowAccumulator.add(indexedColumns.length(),ByteBuffer.wrap(Encoding.encodeBytesUnsorted(kv.getRow())));
            put.add(SpliceConstants.DEFAULT_FAMILY_BYTES,RowMarshaller.PACKED_COLUMN_KEY,rowAccumulator.finish());

            indexPuts.add(put);
        }
        return indexPuts;
    }

    protected void accumulate(EntryAccumulator newKeyAccumulator, BitIndex updateIndex, ByteBuffer newBuffer, int newPos) {
        if(updateIndex.isScalarType(newPos))
            newKeyAccumulator.addScalar(mainColToIndexPosMap[newPos],newBuffer);
        else if(updateIndex.isFloatType(newPos))
            newKeyAccumulator.addFloat(mainColToIndexPosMap[newPos],newBuffer);
        else if(updateIndex.isDoubleType(newPos))
            newKeyAccumulator.addDouble(mainColToIndexPosMap[newPos],newBuffer);
        else
            newKeyAccumulator.add(mainColToIndexPosMap[newPos],newBuffer);
    }
}
