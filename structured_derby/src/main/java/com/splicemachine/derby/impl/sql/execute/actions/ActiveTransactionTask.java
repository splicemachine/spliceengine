package com.splicemachine.derby.impl.sql.execute.actions;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.temp.TempTable;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.marshall.SpreadBucket;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hash.Hash32;
import com.splicemachine.hash.HashFunctions;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.si.api.*;
import com.splicemachine.si.impl.ActiveWriteTxn;
import com.splicemachine.si.impl.Transaction;
import com.splicemachine.si.impl.TransactionStore;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.uuid.Snowflake;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Date: 7/30/14
 */
public class ActiveTransactionTask extends ZkTask {
    private static final Logger LOG = Logger.getLogger(ActiveTransactionTask.class);
    private long minTxnId;
    private long maxTxnId;
    private byte[] writeTable;
    private byte[] operationUUID;
    //the number of active txns to fetch before giving up--allows us to be efficient with our search
    private int numActiveTxns;

    //serialization constructor
    public ActiveTransactionTask() { }

    public ActiveTransactionTask(String jobId,long minTxnId,long maxTxnId,byte[] writeTable, byte[] operationUUID) {
        this(jobId, minTxnId, maxTxnId, writeTable, operationUUID,Integer.MAX_VALUE);
    }

    public ActiveTransactionTask(String jobId,long minTxnId,long maxTxnId,byte[] writeTable, byte[] operationUUID, int numActiveTxns) {
        super(jobId, 0);
        this.minTxnId = minTxnId;
        this.maxTxnId = maxTxnId;
        this.writeTable = writeTable;
        this.operationUUID = operationUUID;
        this.numActiveTxns = numActiveTxns;
    }

    @Override
    protected void doExecute() throws ExecutionException, InterruptedException {
        TxnSupplier txnStore = TransactionStorage.getTxnSupplier();
        /*
         * Get the bucket id for the region.
         *
         * The way the transaction table is built, a region may have an empty start
         * OR an empty end, but will never have both
         */
        byte[] regionKey = region.getStartKey();
        byte bucket;
        if(regionKey.length<=0)
            bucket = 0;
        else
            bucket = regionKey[0];
        byte[] startKey = BytesUtil.concat(Arrays.asList(new byte[]{bucket}, Bytes.toBytes(minTxnId)));
        if(BytesUtil.startComparator.compare(region.getStartKey(),startKey)>0)
            startKey = region.getStartKey();
        byte[] stopKey = BytesUtil.concat(Arrays.asList(new byte[]{bucket}, Bytes.toBytes(maxTxnId)));
        if(BytesUtil.endComparator.compare(region.getEndKey(),stopKey)<0)
            stopKey = region.getEndKey();

        Scan scan = new Scan(startKey,stopKey);
        scan.setFilter(new ActiveTxnFilter(writeTable));
        scan.setMaxVersions(1); //only consider the latest data points

        TempTable tempTable = SpliceDriver.driver().getTempTable();
        SpreadBucket currentSpread = tempTable.getCurrentSpread();
        long l = Snowflake.timestampFromUUID(Bytes.toLong(operationUUID));
        Txn writeTxn =  new ActiveWriteTxn(l,l,Txn.ROOT_TRANSACTION);
        CallBuffer<KVPair> callBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(tempTable.getTempTableName(), writeTxn);
        MultiFieldEncoder keyEncoder = MultiFieldEncoder.create(2);

        Hash32 hashFunction = HashFunctions.murmur3(0);
        byte[] hashBytes = new byte[1+operationUUID.length];
        System.arraycopy(operationUUID,0,hashBytes,1,operationUUID.length);
        boolean[] usedTempBuckets = new boolean[currentSpread.getNumBuckets()];

        int rows = 0;
        RegionScanner scanner = null;
        try {
            scanner = region.getScanner(scan);

            BufferedRegionScanner bufferedRegionScanner = new BufferedRegionScanner(region,scanner,scan,1024, Metrics.noOpMetricFactory());

            List<KeyValue> kvs = Lists.newArrayList();
            boolean shouldContinue;
            do{
                kvs.clear();
                shouldContinue = bufferedRegionScanner.next(kvs);
                if(kvs.size()<=0) break;
                keyEncoder.reset();
                rows++;

                if(!isActiveChildTxn(txnStore,kvs)) continue;
                KeyValue first = kvs.get(0);
                byte[] key = first.getRow();
                //we skip the first byte in the key because it's a Transaction table bucket
                hashBytes[0] = currentSpread.bucket(hashFunction.hash(key, 1, key.length-1));
                usedTempBuckets[currentSpread.bucketIndex(hashBytes[0])] = true; //mark temp bucket as used
                keyEncoder.setRawBytes(hashBytes);
                keyEncoder.setRawBytes(key,1,key.length-1);

                byte[] newKey = keyEncoder.build();
                KVPair kvPair = new KVPair(newKey, HConstants.EMPTY_BYTE_ARRAY); //data will be sorted into
                callBuffer.add(kvPair);

                if(rows>=numActiveTxns)
                    shouldContinue = false;
            }while(shouldContinue);

            callBuffer.flushBuffer();
            callBuffer.close();
            status.setStats(new TaskStats(0l, 0l, 0l, usedTempBuckets)); //TODO -sf- add Stats
        } catch (IOException e) {
            throw new ExecutionException(e);
        } catch (Exception e) {
            throw new ExecutionException(e);
        } finally{
            Closeables.closeQuietly(scanner);
        }
    }

    @Override
    public RegionTask getClone() {
        return new ActiveTransactionTask(jobId,minTxnId,maxTxnId,writeTable,operationUUID);
    }

    @Override public boolean isSplittable() { return false; }

    private boolean isActiveChildTxn(TxnSupplier txnStore,List<KeyValue> kvs) throws IOException {
        /*
         * Returns true if this transaction is actually a child of an active transaction,
         * even if its committed.
         *
         * This is probably really slow (since it will have to do a network lookup for every committed
         * child transaction of a parent). As a result, we will probably need to rewire this at some point.
         */
        for(KeyValue kv:kvs){
            if(matchesQualifier(PARENT_ID,kv.getBuffer(),kv.getQualifierOffset(),kv.getQualifierLength())){
                Txn transaction = txnStore.getTransaction(Bytes.toLong(kv.getBuffer(), kv.getValueOffset(), kv.getValueLength()));
                return transaction.getEffectiveState() == Txn.State.ACTIVE;
            }
        }
        //if we get this far, then we are a parent transaction
        return true;
    }

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
        numActiveTxns = in.readInt();
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
        out.writeInt(numActiveTxns);
    }

    private static final byte[] ACTIVE_STATE = Bytes.toBytes(TransactionStatus.ACTIVE.ordinal());
    private static final byte[] ROLLED_BACK_STATE = Bytes.toBytes(TransactionStatus.ROLLED_BACK.ordinal());
    private static final byte[] GLOBAL_COMMIT_COL = Bytes.toBytes(SIConstants.TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN);
    private static final byte[] COMMIT_TIMESTAMP_COL = Bytes.toBytes(SIConstants.TRANSACTION_COMMIT_TIMESTAMP_COLUMN);
    private static final byte[] PARENT_ID = SIConstants.TRANSACTION_PARENT_COLUMN_BYTES;
    private static final byte[] DEPENDENT = SIConstants.TRANSACTION_DEPENDENT_COLUMN_BYTES;
    private static final byte[] STATUS = Bytes.toBytes(SIConstants.TRANSACTION_STATUS_COLUMN);
    private static final byte[] ALLOWS_WRITES = SIConstants.TRANSACTION_ALLOW_WRITES_COLUMN_BYTES;
    private static final byte[] WRITE_TABLE = Bytes.toBytes(SIConstants.WRITE_TABLE_COLUMN);

    private static boolean matchesQualifier(byte[] qualifier, byte[] buffer, int qualifierOffset, int qualifierLength) {
        return Bytes.equals(qualifier, 0, qualifier.length, buffer, qualifierOffset, qualifierLength);
    }

    private static class ActiveTxnFilter extends FilterBase{
        private final byte[] writeTable;

        private boolean filter = false;
        private boolean dependent = true;
        private boolean committed = false;
        private boolean isChild = false;
        private boolean writeTableSeen = false;

        private ActiveTxnFilter(byte[] writeTable) {
            this.writeTable = writeTable;
        }

        @Override
        public ReturnCode filterKeyValue(KeyValue kv) {
            /*
             * The logic is as follows:
             *
             * 1. If it does not affect the specified table, disregard.
             * 2. If the transaction does not allow writes, disregard.
             * 3. If the transaction is rolled back, disregard
             * 4. If the transaction's globalCommitTimestamp or effectiveCommitTimestamp is set, disregard (it has been fully committed).
             * 5. If the transaction is independent(dependent = false || parentTxnId==null) and COMMITTED, disregard
             * 6. otherwise, include
             *
             * The ordering of columns implies that it will
             * check dependence first, then allowing writes, then status, then
             * global commit timestamp, then write table.
             */
            byte[] buffer = kv.getBuffer();
            int valueOffset = kv.getValueOffset();
            int valueLength = kv.getValueLength();

            int qualifierOffset = kv.getQualifierOffset();
            int qualifierLength = kv.getQualifierLength();
            if(matchesQualifier(ALLOWS_WRITES,buffer, qualifierOffset, qualifierLength)){
                if(!BytesUtil.toBoolean(buffer, valueOffset)){
                    //this transaction does not allow writes, ignore
                    filter = true;
                    return ReturnCode.NEXT_ROW;
                }
                return ReturnCode.INCLUDE;
            }else if(matchesQualifier(STATUS,buffer,qualifierOffset,qualifierLength)){
                if(Bytes.equals(ROLLED_BACK_STATE,0,ROLLED_BACK_STATE.length, buffer, valueOffset, valueLength)){
                    //this transaction has been rolled back, disregard
                    filter = true;
                    return ReturnCode.NEXT_ROW;
                }
                return ReturnCode.INCLUDE;
            }else if(matchesQualifier(GLOBAL_COMMIT_COL,buffer,qualifierOffset,qualifierLength)){
                //this transaction has a global commit timestamp, disregard, it's been committed
                filter = true;
                return ReturnCode.NEXT_ROW;
            }else if(writeTable!=null && matchesQualifier(WRITE_TABLE,buffer,qualifierOffset,qualifierLength)){
                writeTableSeen = true;
                if(!Bytes.equals(writeTable,0,writeTable.length,buffer,valueOffset,valueLength)){
                    filter = true;
                    return ReturnCode.NEXT_ROW;
                }else return ReturnCode.INCLUDE;
            }else if(matchesQualifier(COMMIT_TIMESTAMP_COL,buffer,qualifierOffset,qualifierLength)){
                committed=true;
                if(!dependent){
                    filter = true;
                    return ReturnCode.NEXT_ROW;
                }
                return ReturnCode.INCLUDE;
            }else if(matchesQualifier(DEPENDENT,buffer,qualifierOffset,qualifierLength)){
                dependent = BytesUtil.toBoolean(buffer,valueOffset);
                if(committed &&!dependent){
                    filter = true;
                    return ReturnCode.NEXT_ROW;
                }
                return ReturnCode.INCLUDE;
            }else if(matchesQualifier(PARENT_ID,buffer,qualifierOffset,qualifierLength)){
                isChild=true;
                return ReturnCode.INCLUDE;
            } else{
                //this column is not relevant, throw it in for decoding
                return ReturnCode.INCLUDE;
            }
        }



        @Override
        public boolean filterRow() {
            if(filter) return true; //if we have explicitely decided it doesn't apply
            else if (!isChild && committed) return true; //if it's a committed parent transaction, discard
            else if(writeTable!=null && !writeTableSeen) return true; //if we have a write table, but never saw the write table column
            else return false;
        }

        @Override
        public void reset() {
            filter = false;
            isChild = false;
            committed = false;
            dependent = true;
            writeTableSeen = false;
        }

        @Override
        public void write(DataOutput dataOutput) throws IOException {
           //no need
        }

        @Override
        public void readFields(DataInput dataInput) throws IOException {
            //no need
        }
    }
}
