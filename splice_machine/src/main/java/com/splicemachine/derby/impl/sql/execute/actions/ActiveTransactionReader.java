package com.splicemachine.derby.impl.sql.execute.actions;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.SpliceZeroCopyByteString;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.storage.DistributedScanner;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.temp.TempTable;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.marshall.BucketHasher;
import com.splicemachine.hbase.FilteredRowKeyDistributor;
import com.splicemachine.hbase.MeasuredResultScanner;
import com.splicemachine.hbase.RowKeyDistributor;
import com.splicemachine.hbase.RowKeyDistributorByHashPrefix;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobStats;
import com.splicemachine.job.Task;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.coprocessor.TxnMessage;
import com.splicemachine.si.impl.TransactionStorage;
import com.splicemachine.si.impl.TxnViewBuilder;
import com.splicemachine.stream.AbstractStream;
import com.splicemachine.stream.Stream;
import com.splicemachine.stream.StreamException;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Date: 7/30/14
 */
public class ActiveTransactionReader {
    private final long minTxnId;
    private final long maxTxnId;
    private final byte[] writeTable;

    public ActiveTransactionReader(long minTxnId, long maxTxnId, byte[] writeTable){
        this.minTxnId = minTxnId;
        this.maxTxnId = maxTxnId;
        this.writeTable = writeTable;
    }

    public Stream<TxnView> getAllTransactions() throws IOException{
        return getAllTransactions(SpliceConstants.DEFAULT_CACHE_SIZE);
    }

    public Stream<TxnView> getActiveTransactions() throws IOException{
        return getActiveTransactions(SpliceConstants.DEFAULT_CACHE_SIZE);
    }

    public Stream<TxnView> getAllTransactions(int queueSize) throws IOException{
        byte[] operationId = SpliceDriver.driver().getUUIDGenerator().nextUUIDBytes();
        ActiveTxnJob job = new ActiveTxnJob(operationId, queueSize,false);
        return runJobAndScan(job);
    }

    public Stream<TxnView> getActiveTransactions(int queueSize) throws IOException{
        byte[] operationId = SpliceDriver.driver().getUUIDGenerator().nextUUIDBytes();
        ActiveTxnJob job = new ActiveTxnJob(operationId, queueSize);
        return runJobAndScan(job);
    }

    protected Stream<TxnView> runJobAndScan(ActiveTxnJob job) throws IOException {
        try {
            final JobFuture submit = SpliceDriver.driver().getJobScheduler().submit(job);
            submit.completeAll(null);
            JobStats jobStats = submit.getJobStats();

            boolean[] usedTempBuckets = getUsedTempBuckets(jobStats);
            final MeasuredResultScanner scanner = configureResultScan(job.operationUUID, usedTempBuckets, submit);
            return new AbstractStream<TxnView>(){
                @Override
                public TxnView next() throws StreamException{
                    try{
                        Result r = scanner.next();
                        if(r==null) return null;
                        return decode(r);
                    }catch(IOException e){
                        throw new StreamException(e);
                    }
                }

                @Override public void close() throws StreamException{
                    try {
                        scanner.close();
                    }finally {
                        //clean up the job since we're done reading it
                        try {
                            submit.cleanup();
                        } catch (ExecutionException e) {
                            Logger.getLogger(ActiveTransactionReader.class).warn("Unable to cleanup job "+ submit.toString());
                        }
                    }
                }
            };
        } catch (ExecutionException | InterruptedException e) {
            throw Exceptions.getIOException(e);
        }
    }

    private TxnView decode(Result r) throws StreamException{
        Cell[] cells = r.rawCells();
        Cell data = cells[0];

        TxnMessage.Txn txn;
        try {
            txn = TxnMessage.Txn.parseFrom(SpliceZeroCopyByteString.wrap(data.getValueArray(),data.getValueOffset(),data.getValueLength()));
        } catch (InvalidProtocolBufferException e) {
            throw new StreamException(e); //shouldn't happen
        }
        TxnMessage.TxnInfo info = txn.getInfo();
        TxnViewBuilder tvb = new TxnViewBuilder().txnId(info.getTxnId())
                .parentTxnId(info.getParentTxnid())
                .beginTimestamp(info.getBeginTs())
                .commitTimestamp(txn.getCommitTs())
                .globalCommitTimestamp(txn.getGlobalCommitTs())
                .state(com.splicemachine.si.api.Txn.State.fromInt(txn.getState()))
                .isolationLevel(com.splicemachine.si.api.Txn.IsolationLevel.fromInt(info.getIsolationLevel()))
                .keepAliveTimestamp(txn.getLastKeepAliveTime())
                .store(TransactionStorage.getTxnSupplier());

        if(info.hasDestinationTables())
            tvb = tvb.destinationTable(info.getDestinationTables().toByteArray());
        if(info.hasIsAdditive())
            tvb = tvb.additive(info.getIsAdditive());

        try {
            return tvb.build();
        } catch (IOException e) {
            throw new StreamException(e);
        }
    }

    /*private helper methods*/
    
    private MeasuredResultScanner configureResultScan(byte[] operationId,boolean[] usedTempBuckets,final JobFuture submit) throws IOException {
        final TempTable tempTable = SpliceDriver.driver().getTempTable();
        RowKeyDistributor keyDistributor = new RowKeyDistributorByHashPrefix(BucketHasher.getHasher(tempTable.getCurrentSpread()));
        keyDistributor = new FilteredRowKeyDistributor(keyDistributor,usedTempBuckets);

        Scan scan = new Scan();
        scan.setStartRow(operationId);
        scan.setStopRow(BytesUtil.unsignedCopyAndIncrement(operationId));
        return DistributedScanner.create(SpliceAccessManager.getHTable(tempTable.getTempTableName()),scan,keyDistributor,Metrics.noOpMetricFactory());
    }

    private boolean[] getUsedTempBuckets(JobStats jobStats) {
        List<TaskStats> taskStats = jobStats.getTaskStats();
        boolean[] usedTempBuckets = new boolean[SpliceDriver.driver().getTempTable().getCurrentSpread().getNumBuckets()];
        for(TaskStats stats:taskStats){
            boolean[] utb = stats.getTempBuckets();
            for(int i=0;i<usedTempBuckets.length;i++){
                usedTempBuckets[i] =usedTempBuckets[i]||utb[i];
            }
        }
        return usedTempBuckets;
    }


    private class ActiveTxnJob implements CoprocessorJob {
        private final byte[] operationUUID;
        private final int queueSize;
        private final boolean activeOnly;

        private ActiveTxnJob(byte[] operationUUID, int queueSize) {
            this(operationUUID, queueSize,true);
        }

        private ActiveTxnJob(byte[] operationUUID, int queueSize,boolean activeOnly) {
            this.operationUUID = operationUUID;
            this.queueSize = queueSize;
            this.activeOnly = activeOnly;
        }

        @Override
        public String getJobId() {
            return String.format("ActiveTransactions-%d", Bytes.toLong(operationUUID));
        }

        @Override
        public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
            TransactionReadTask task = new TransactionReadTask(getJobId(), minTxnId, maxTxnId, writeTable, operationUUID,queueSize,activeOnly);
            return Collections.singletonMap(task,Pair.newPair(HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW));
        }

        @Override
        public HTableInterface getTable() {
            return SpliceAccessManager.getHTable(SIConstants.TRANSACTION_TABLE_BYTES);
        }

        @Override public byte[] getDestinationTable() { return SIConstants.TRANSACTION_TABLE_BYTES; }

        @Override public Txn getTxn() { return null; }

        @Override
        public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) throws IOException {
            return Pair.newPair(originalTask,Pair.newPair(taskStartKey,taskEndKey));
        }
    }
}
