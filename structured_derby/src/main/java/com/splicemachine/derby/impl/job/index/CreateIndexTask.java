package com.splicemachine.derby.impl.job.index;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.sql.execute.index.WriteContextFactoryPool;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.hbase.CallBuffer;
import com.splicemachine.hbase.MutationRequest;
import com.splicemachine.hbase.MutationResponse;
import com.splicemachine.hbase.TableWriter;
import com.splicemachine.hbase.batch.WriteContextFactory;
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
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/5/13
 */
public class CreateIndexTask extends ZkTask {
    private static final long serialVersionUID = 2l;
    private String transactionId;
    private long indexConglomId;
    private long baseConglomId;
    private int[] indexColsToBaseColMap;
    private boolean isUnique;

    private HRegion region;

    public CreateIndexTask() {
    }

    public CreateIndexTask(String transactionId,
                           long indexConglomId,
                           long baseConglomId,
                           int[] indexColsToBaseColMap,
                           boolean unique,
                           String jobId ) {
        super(jobId, OperationJob.operationTaskPriority,transactionId,false);
        this.transactionId = transactionId;
        this.indexConglomId = indexConglomId;
        this.baseConglomId = baseConglomId;
        this.indexColsToBaseColMap = indexColsToBaseColMap;
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
        ArrayUtil.writeIntArray(out, indexColsToBaseColMap);
        out.writeBoolean(isUnique);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        transactionId = in.readUTF();
        indexConglomId = in.readLong();
        baseConglomId = in.readLong();
        indexColsToBaseColMap = ArrayUtil.readIntArray(in);
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

        for(int mainTablePos:indexColsToBaseColMap){
            regionScan.addColumn(SpliceConstants.DEFAULT_FAMILY_BYTES, Encoding.encode(mainTablePos - 1));
        }

        try{
            //add index to table watcher
            WriteContextFactory contextFactory = WriteContextFactoryPool.getContextFactory(baseConglomId);
            contextFactory.addIndex(indexConglomId, indexColsToBaseColMap, isUnique);
            
            //backfill the index with previously committed data
            RegionScanner sourceScanner = region.getScanner(regionScan);

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
                            for(String failureMessage:response.getFailedRows().values()){
                                if(failureMessage.contains("NotServingRegion")||failureMessage.contains("WrongRegion"))
                                    return Response.RETRY;
                            }
                            return  Response.THROW_ERROR;
                        }
                    });

            List < KeyValue > nextRow = Lists.newArrayListWithExpectedSize(indexColsToBaseColMap.length);
            //translate down to zero-indexed
            int[] indexColMap = new int[indexColsToBaseColMap.length];
            for(int pos=0;pos<indexColsToBaseColMap.length;pos++){
                indexColMap[pos] = indexColsToBaseColMap[pos]-1;
            }
            boolean shouldContinue = true;
            while(shouldContinue){
                nextRow.clear();
                shouldContinue  = sourceScanner.next(nextRow);
                List<Put> indexPuts = translateResult(nextRow,indexColMap);

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

    private List<Put> translateResult(List<KeyValue> result,int[] indexColsToMainColMap) throws IOException{
        Map<byte[],List<KeyValue>> putConstructors = Maps.newHashMapWithExpectedSize(1);
        for(KeyValue keyValue:result){
            List<KeyValue> cols = putConstructors.get(keyValue.getRow());
            if(cols==null){
                cols = Lists.newArrayListWithExpectedSize(indexColsToMainColMap.length);
                putConstructors.put(keyValue.getRow(),cols);
            }
            cols.add(keyValue);
        }
        //build Puts for each row
        List<Put> indexPuts = Lists.newArrayListWithExpectedSize(putConstructors.size());
        for(byte[] mainRow: putConstructors.keySet()){
            List<KeyValue> rowData = putConstructors.get(mainRow);
            byte[][] indexRowData = getDataArray();
            int rowSize=0;
            for(KeyValue kv:rowData){
                int colPos = Encoding.decodeInt(kv.getQualifier());
                for(int indexPos=0;indexPos<indexColsToMainColMap.length;indexPos++){
                    if(colPos == indexColsToMainColMap[indexPos]){
                        byte[] val = kv.getValue();
                        indexRowData[indexPos] = val;
                        rowSize+=val.length;
                        break;
                    }
                }
            }
            if(!isUnique){
                byte[] postfix = SpliceUtils.getUniqueKey();
                indexRowData[indexRowData.length-1] = postfix;
                rowSize+=postfix.length;
            }

            byte[] finalIndexRow = new byte[rowSize];
            int offset =0;
            for(byte[] indexCol:indexRowData){
                System.arraycopy(indexCol,0,finalIndexRow,offset,indexCol.length);
                offset+=indexCol.length;
            }
            Put indexPut = SpliceUtils.createPut(finalIndexRow, transactionId);
            for(int dataPos=0;dataPos<indexRowData.length;dataPos++){
                byte[] putPos = Encoding.encode(dataPos);
                indexPut.add(DEFAULT_FAMILY_BYTES,putPos,indexRowData[dataPos]);
            }

            indexPut.add(DEFAULT_FAMILY_BYTES,
            		Encoding.encode(rowData.size()),mainRow);
            indexPuts.add(indexPut);
        }

        return indexPuts;
    }

    private byte[][] getDataArray() {
        if(isUnique)
            return new byte[indexColsToBaseColMap.length][];
        else
            return new byte[indexColsToBaseColMap.length+1][];
    }
}
