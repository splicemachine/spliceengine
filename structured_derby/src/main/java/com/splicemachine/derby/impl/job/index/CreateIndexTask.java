package com.splicemachine.derby.impl.job.index;

import com.google.common.collect.Lists;
import com.splicemachine.constants.HBaseConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZooKeeperTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.sql.execute.index.IndexManager;
import com.splicemachine.derby.impl.sql.execute.index.IndexSetPool;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.CallBuffer;
import org.apache.derby.iapi.services.io.ArrayUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/5/13
 */
public class CreateIndexTask extends ZooKeeperTask {
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
                           boolean unique,String jobId ) {
        super(jobId, OperationJob.operationTaskPriority);
        this.transactionId = transactionId;
        this.indexConglomId = indexConglomId;
        this.baseConglomId = baseConglomId;
        this.indexColsToBaseColMap = indexColsToBaseColMap;
        isUnique = unique;
    }

    @Override
    public void prepareTask(HRegion region, RecoverableZooKeeper zooKeeper) throws ExecutionException {
        this.region = region;
        super.prepareTask(region, zooKeeper);
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
        regionScan.setCaching(100);
        regionScan.setStartRow(region.getStartKey());
        regionScan.setStopRow(region.getEndKey());

        for(int mainTablePos:indexColsToBaseColMap){
            regionScan.addColumn(HBaseConstants.DEFAULT_FAMILY_BYTES,Integer.toString(mainTablePos-1).getBytes());
        }

        try{
            RegionScanner sourceScanner = region.getScanner(regionScan);

            IndexManager indexManager = IndexManager.create(indexConglomId,indexColsToBaseColMap,isUnique);

            CallBuffer<Mutation> writeBuffer =
                    SpliceDriver.driver().getTableWriter().writeBuffer(Long.toString(indexConglomId).getBytes());

            List<KeyValue> nextRow = Lists.newArrayListWithExpectedSize(indexColsToBaseColMap.length);
            boolean shouldContinue = true;
            while(shouldContinue){
                nextRow.clear();
                shouldContinue  = sourceScanner.next(nextRow);
                List<Put> indexPuts = indexManager.translateResult(transactionId,nextRow);

                writeBuffer.addAll(indexPuts);
            }
            writeBuffer.flushBuffer();
            writeBuffer.close();

            IndexSetPool.getIndex(baseConglomId).addIndex(indexManager);
        } catch (IOException e) {
            throw new ExecutionException(e);
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }
}
