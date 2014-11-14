package com.splicemachine.derby.impl.sql.execute.actions;

import com.google.common.io.Closeables;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.temp.TempTable;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.marshall.SpreadBucket;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hash.Hash32;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.CallBuffer;
import com.splicemachine.si.api.*;
import com.splicemachine.si.coprocessors.TxnLifecycleEndpoint;
import com.splicemachine.si.impl.*;
import com.splicemachine.si.impl.region.RegionTxnStore;
import com.splicemachine.utils.Source;
import com.splicemachine.utils.SpliceZooKeeperManager;
import com.splicemachine.uuid.Snowflake;

import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Date: 7/30/14
 */
public class TransactionReadTask extends ZkTask {
    private static final Logger LOG = Logger.getLogger(TransactionReadTask.class);
    private static SIFactory siFactory = SIFactoryDriver.siFactory;
    private long minTxnId;
    private long maxTxnId;
    private byte[] writeTable;
    private byte[] operationUUID;
    //the number of active txns to fetch before giving up--allows us to be efficient with our search
    private int numTxns;
    private boolean activeOnly; //when true, will fetch only active transactions    
    private RegionTxnStore regionTxnStore;

    //serialization constructor
    public TransactionReadTask() { }

    public TransactionReadTask(String jobId, long minTxnId, long maxTxnId, byte[] writeTable, byte[] operationUUID) {
        this(jobId, minTxnId, maxTxnId, writeTable, operationUUID,Integer.MAX_VALUE,true);
    }

    public TransactionReadTask(String jobId, long minTxnId, long maxTxnId, byte[] writeTable, byte[] operationUUID, int numTxns) {
        this(jobId,minTxnId,maxTxnId,writeTable,operationUUID,numTxns,true);
    }

    public TransactionReadTask(String jobId, long minTxnId, long maxTxnId, byte[] writeTable,
                               byte[] operationUUID, int numTxns,boolean activeOnly) {
        super(jobId, 0);
        this.minTxnId = minTxnId;
        this.maxTxnId = maxTxnId;
        this.writeTable = writeTable;
        this.operationUUID = operationUUID;
        this.numTxns = numTxns;
        this.activeOnly = activeOnly;
    }

    @Override
    public void prepareTask(byte[] start, byte[] stop, RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
        super.prepareTask(start, stop, rce, zooKeeper);
        TxnLifecycleEndpoint tle = (TxnLifecycleEndpoint)rce.getRegion().getCoprocessorHost().findCoprocessor(TxnLifecycleEndpoint.class.getName());
        regionTxnStore = tle.getRegionTxnStore();
    }

    @Override
    protected void doExecute() throws ExecutionException, InterruptedException {
        TempTable tempTable = SpliceDriver.driver().getTempTable();
        SpreadBucket currentSpread = tempTable.getCurrentSpread();
        long l = Snowflake.timestampFromUUID(Bytes.toLong(operationUUID));
        TxnView writeTxn =  new ActiveWriteTxn(l,l,Txn.ROOT_TRANSACTION);
        CallBuffer<KVPair> callBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(tempTable.getTempTableName(), writeTxn);
        MultiFieldEncoder keyEncoder = MultiFieldEncoder.create(2);

        Hash32 hashFunction = HashFunctions.murmur3(0);
        byte[] hashBytes = new byte[1+operationUUID.length];
        System.arraycopy(operationUUID,0,hashBytes,1,operationUUID.length);
        boolean[] usedTempBuckets = new boolean[currentSpread.getNumBuckets()];

        int rows = 0;
        Source activeTxns = null;
        try{
            activeTxns = activeOnly?regionTxnStore.getActiveTxns(minTxnId, maxTxnId, writeTable):
                    regionTxnStore.getAllTxns(minTxnId,maxTxnId); //todo -sf- add destination table filter

            MultiFieldEncoder rowEncoder = MultiFieldEncoder.create(11);
            while(activeTxns.hasNext()){
            	Object txn = activeTxns.next();
                byte[] key = Encoding.encode(siFactory.getTxnId(txn));
                hashBytes[0] = currentSpread.bucket(hashFunction.hash(key,0,key.length));
                usedTempBuckets[currentSpread.bucketIndex(hashBytes[0])] = true;
                keyEncoder.reset();
                key = keyEncoder.setRawBytes(hashBytes).setRawBytes(key).build();
                KVPair kvPair = new KVPair(key,siFactory.transactionToByteArray(rowEncoder, txn));
                callBuffer.add(kvPair);

                if(rows>= numTxns)
                    break;
            }
            callBuffer.flushBuffer();
            callBuffer.close();
            status.setStats(new TaskStats(0l, 0l, 0l, usedTempBuckets)); //TODO -sf- add Stats
        } catch (Exception e) {
            throw new ExecutionException(e);
        } finally{
            Closeables.closeQuietly(activeTxns);
        }
    }

    @Override
    public RegionTask getClone() {
        return new TransactionReadTask(jobId,minTxnId,maxTxnId,writeTable,operationUUID);
    }

    @Override public boolean isSplittable() { return false; }

    @Override protected String getTaskType() { return "ActiveTransaction"; }
    @Override public boolean invalidateOnClose() { return true; }
    @Override public int getPriority() { return 0; }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        minTxnId = in.readLong();
        maxTxnId = in.readLong();
        if(in.readBoolean()){
            writeTable = new byte[in.readInt()];
            in.readFully(writeTable);
        }
        operationUUID = new byte[in.readInt()];
        in.readFully(operationUUID);
        numTxns = in.readInt();
        activeOnly = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeLong(minTxnId);
        out.writeLong(maxTxnId);
        out.writeBoolean(writeTable!=null);
        if(writeTable!=null){
            out.writeInt(writeTable.length);
            out.write(writeTable);
        }
        out.writeInt(operationUUID.length);
        out.write(operationUUID);
        out.writeInt(numTxns);
        out.writeBoolean(activeOnly);
    }
}
