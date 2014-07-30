package com.splicemachine.derby.impl.sql.execute.actions;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.temp.TempTable;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.marshall.SpreadBucket;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.hbase.KeyValueUtils;
import com.splicemachine.hbase.writer.CallBuffer;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.stats.Metrics;
import com.splicemachine.utils.hash.ByteHash32;
import com.splicemachine.utils.hash.HashFunctions;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.*;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Date: 7/30/14
 */
public class ActiveTransactionTask extends ZkTask {

    private long minTxnId;
    private long maxTxnId;
    private byte[] writeTable;
    private byte[] operationUUID;

    //serialization constructor
    public ActiveTransactionTask() { }

    public ActiveTransactionTask(String jobId,long minTxnId,long maxTxnId,byte[] writeTable, byte[] operationUUID) {
        super(jobId, 0, null, true);
        this.minTxnId = minTxnId;
        this.maxTxnId = maxTxnId;
        this.writeTable = writeTable;
        this.operationUUID = operationUUID;
    }

    @Override
    protected void doExecute() throws ExecutionException, InterruptedException {
        //get a scanner
        //get the bucket id for the region
        /*
         * Get the bucket id for the region.
         *
         * The way the transaction table is built, a region may have an empty start
         * OR an empty end, but will never have both
         */
        byte[] regionKey = region.getStartKey();
        if(regionKey.length<=0)
            regionKey = region.getEndKey();
        byte bucket = regionKey[0];
        byte[] startKey = BytesUtil.concat(Arrays.asList(new byte[]{bucket}, Bytes.toBytes(minTxnId)));
        if(BytesUtil.startComparator.compare(region.getStartKey(),startKey)>0)
            startKey = region.getStartKey();
        byte[] stopKey = BytesUtil.concat(Arrays.asList(new byte[]{bucket}, Bytes.toBytes(maxTxnId)));
        if(BytesUtil.endComparator.compare(region.getEndKey(),stopKey)<0)
            stopKey = region.getEndKey();

        Scan scan = new Scan(startKey,stopKey);
        scan.setFilter(new ActiveTxnFilter(writeTable));

        TempTable tempTable = SpliceDriver.driver().getTempTable();
        SpreadBucket currentSpread = tempTable.getCurrentSpread();
        String txnId = Long.toString(Bytes.toLong(operationUUID));
        CallBuffer<KVPair> callBuffer = SpliceDriver.driver().getTableWriter().writeBuffer(tempTable.getTempTableName(), txnId);
        MultiFieldEncoder keyEncoder = MultiFieldEncoder.create(3);

        ByteHash32 hashFunction = HashFunctions.murmur3(0);
        byte[] hashBytes = new byte[1];
        boolean[] usedTempBuckets = new boolean[currentSpread.getNumBuckets()];

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

                KeyValue first = kvs.get(0);
                byte[] key = first.getKey();
                //we skip the first byte in the key because it's a Transaction table bucket
                hashBytes[0] = currentSpread.bucket(hashFunction.hash(key, 1, key.length));
                usedTempBuckets[hashBytes[0]] = true; //mark temp bucket as used
                keyEncoder.setRawBytes(hashBytes);
                keyEncoder.setRawBytes(operationUUID);
                keyEncoder.setRawBytes(key,1,key.length);

                KVPair kvPair = new KVPair(key, HConstants.EMPTY_BYTE_ARRAY); //data will be sorted into
                callBuffer.add(kvPair);
            }while(shouldContinue);

            callBuffer.flushBuffer();
            callBuffer.close();
        } catch (IOException e) {
            throw new ExecutionException(e);
        } catch (Exception e) {
            throw new ExecutionException(e);
        } finally{
            Closeables.closeQuietly(scanner);

            status.setStats(new TaskStats(0l,0l,0l,usedTempBuckets)); //TODO -sf- add Stats
        }
    }

    @Override protected String getTaskType() { return "ActiveTransaction"; }
    @Override public boolean invalidateOnClose() { return true; }
    @Override public int getPriority() { return 0; }
    @Override public boolean isTransactional() { return false; }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        minTxnId = in.readLong();
        maxTxnId = in.readLong();
        writeTable = new byte[in.readInt()];
        in.readFully(writeTable);
        operationUUID = new byte[in.readInt()];
        in.readFully(operationUUID);
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeLong(minTxnId);
        out.writeLong(maxTxnId);
        out.writeInt(writeTable.length);
        out.write(writeTable);
        out.writeInt(operationUUID.length);
        out.write(operationUUID);
    }

    private static final byte[] ACTIVE_STATE = Bytes.toBytes(TransactionStatus.ACTIVE.ordinal());
    private static final byte[] ROLLED_BACK_STATE = Bytes.toBytes(TransactionStatus.ROLLED_BACK.ordinal());
    private static final byte[] GLOBAL_COMMIT_COL = Bytes.toBytes(SIConstants.TRANSACTION_GLOBAL_COMMIT_TIMESTAMP_COLUMN);
    private static final byte[] STATUS = Bytes.toBytes(SIConstants.TRANSACTION_STATUS_COLUMN);
    private static final byte[] ALLOWS_WRITES = Bytes.toBytes(SIConstants.TRANSACTION_ALLOW_WRITES_COLUMN);
    private static final byte[] WRITE_TABLE = Bytes.toBytes(SIConstants.WRITE_TABLE_COLUMN);

    private static class ActiveTxnFilter extends FilterBase{
        private final byte[] writeTable;

        private boolean filter = false;

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
            if(KeyValueUtils.singleMatchingColumn(kv,SIConstants.TRANSACTION_FAMILY_BYTES, ALLOWS_WRITES)){
                if(!BytesUtil.toBoolean(buffer, valueOffset)){
                    //this transaction does not allow writes, ignore
                    filter = true;
                    return ReturnCode.NEXT_ROW;
                }
                return ReturnCode.INCLUDE;
            }else if(KeyValueUtils.singleMatchingColumn(kv,SIConstants.TRANSACTION_FAMILY_BYTES,STATUS)){
                if(Bytes.equals(ROLLED_BACK_STATE,0,ROLLED_BACK_STATE.length, buffer, valueOffset, valueLength)){
                    //this transaction has been rolled back, disregard
                    filter = true;
                    return ReturnCode.NEXT_ROW;
                }
                return ReturnCode.INCLUDE;
            }else if(KeyValueUtils.singleMatchingColumn(kv,SIConstants.TRANSACTION_FAMILY_BYTES,GLOBAL_COMMIT_COL)){
                //this transaction has a global commit timestamp, disregard, it's been committed
                filter = true;
                return ReturnCode.NEXT_ROW;
            }else if(KeyValueUtils.singleMatchingColumn(kv,SIConstants.TRANSACTION_FAMILY_BYTES,WRITE_TABLE)){
                if(!Bytes.equals(writeTable,0,writeTable.length,buffer,valueOffset,valueLength)){
                    filter = true;
                    return ReturnCode.NEXT_ROW;
                }else return ReturnCode.INCLUDE;
            }else{
                //this column is not relevant, throw it in for decoding
                return ReturnCode.INCLUDE;
            }
        }

        @Override
        public boolean filterRow() {
            return filter;
        }

        @Override
        public void reset() {
            filter = false;
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
