package com.splicemachine.derby.impl.job.index;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.sql.execute.LocalWriteContextFactory;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.marshall.RowMarshaller;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.*;
import com.splicemachine.hbase.batch.WriteContext;
import com.splicemachine.hbase.writer.KVPair;
import com.splicemachine.hbase.writer.MutationResult;
import com.splicemachine.hbase.writer.WriteResult;
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
    private static final long serialVersionUID = 4l;
    private String transactionId;
    private long indexConglomId;
    private long baseConglomId;
    private int[] mainColToIndexPosMap;
    private boolean isUnique;
    private BitSet indexedColumns;
    private BitSet nonUniqueIndexColumns;
    private BitSet descColumns;

    private MultiFieldEncoder translateEncoder;

    private HRegion region;
    private RegionCoprocessorEnvironment rce;

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
        this.rce = rce;
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
        out.writeObject(descColumns);
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
        descColumns = (BitSet)in.readObject();
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
            LocalWriteContextFactory contextFactory = SpliceIndexEndpoint.getContextFactory(baseConglomId);
            contextFactory.addIndex(indexConglomId, indexedColumns,mainColToIndexPosMap, isUnique,descColumns);

            //backfill the index with previously committed data
            RegionScanner sourceScanner = region.getCoprocessorHost().preScannerOpen(regionScan);
            if(sourceScanner==null)
                sourceScanner = region.getScanner(regionScan);
            try{
                List<KeyValue> nextRow = Lists.newArrayListWithExpectedSize(mainColToIndexPosMap.length);
                boolean shouldContinue = true;
                WriteContext indexOnlyWriteHandler = contextFactory.getIndexOnlyWriteHandler(getTaskStatus().getTransactionId(),indexConglomId, rce);
                while(shouldContinue){
                    nextRow.clear();
                    shouldContinue  = sourceScanner.next(nextRow);
                    translateResult(nextRow, indexOnlyWriteHandler);
                }
                Map<KVPair,WriteResult> finish = indexOnlyWriteHandler.finish();
                for(WriteResult result:finish.values()){
                    if(result.getCode()== WriteResult.Code.FAILED){
                        throw new IOException(result.getErrorMessage());
                    }
                }
            }finally{
                sourceScanner.close();
            }

        } catch (IOException e) {
            throw new ExecutionException(e);
        } catch (Exception e) {
            throw new ExecutionException(Throwables.getRootCause(e));
        }
    }

    private void translateResult(List<KeyValue> result,WriteContext ctx) throws IOException{
        //we know that there is only one KeyValue for each row
        Put currentPut;
        for(KeyValue kv:result){
            //ignore SI CF
            if(kv.matchingFamily(SIConstants.SNAPSHOT_ISOLATION_FAMILY_BYTES)) continue;

            byte[] row = kv.getRow();
            byte[] data = kv.getValue();
            ctx.sendUpstream(new KVPair(row,data));
        }
    }

    protected void accumulate(EntryAccumulator newKeyAccumulator, BitIndex updateIndex, ByteBuffer newBuffer, int newPos) {
        if(updateIndex.isScalarType(newPos))
            newKeyAccumulator.addScalar(newPos, newBuffer);
        else if(updateIndex.isFloatType(newPos))
            newKeyAccumulator.addFloat(newPos,newBuffer);
        else if(updateIndex.isDoubleType(newPos))
            newKeyAccumulator.addDouble(newPos,newBuffer);
        else
            newKeyAccumulator.add(newPos,newBuffer);
    }
}
