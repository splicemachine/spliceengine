package com.splicemachine.derby.impl.sql.execute.actions;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.splicemachine.collections.CloseableIterator;
import com.splicemachine.collections.ForwardingCloseableIterator;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.storage.DerbyAsyncScannerUtils;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.impl.temp.TempTable;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.marshall.BucketHasher;
import com.splicemachine.hbase.FilteredRowKeyDistributor;
import com.splicemachine.hbase.RowKeyDistributor;
import com.splicemachine.hbase.RowKeyDistributorByHashPrefix;
import com.splicemachine.hbase.async.AsyncScanner;
import com.splicemachine.hbase.async.SimpleAsyncScanner;
import com.splicemachine.hbase.async.SortedGatheringScanner;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobStats;
import com.splicemachine.job.Task;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.metrics.Metrics;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.hbase.async.HBaseClient;
import org.hbase.async.Scanner;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 7/30/14
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

    public CloseableIterator<Long> getActiveTransactionIds() throws IOException{
        return getActiveTransactionIds(SpliceConstants.DEFAULT_CACHE_SIZE);
    }

    public CloseableIterator<Long> getActiveTransactionIds(int queueSize) throws IOException{
        byte[] operationId = SpliceDriver.driver().getUUIDGenerator().nextUUIDBytes();
        try {
            final JobFuture submit = SpliceDriver.driver().getJobScheduler().submit(new ActiveTxnJob(operationId,queueSize));
            submit.completeAll(null);
            JobStats jobStats = submit.getJobStats();

            boolean[] usedTempBuckets = getUsedTempBuckets(jobStats);
            final TempTable tempTable = SpliceDriver.driver().getTempTable();
            RowKeyDistributor keyDistributor = new RowKeyDistributorByHashPrefix(BucketHasher.getHasher(tempTable.getCurrentSpread()));
            keyDistributor = new FilteredRowKeyDistributor(keyDistributor,usedTempBuckets);

            Scan scan = new Scan();
            scan.setStartRow(operationId);
            scan.setStopRow(BytesUtil.unsignedCopyAndIncrement(operationId));
            final HBaseClient hBaseClient = SimpleAsyncScanner.HBASE_CLIENT;
            final AsyncScanner scanner = SortedGatheringScanner.newScanner(scan,queueSize, Metrics.noOpMetricFactory(), new Function<Scan, Scanner>() {
                @Nullable
                @Override
                public Scanner apply(@Nullable Scan input) {
                    return DerbyAsyncScannerUtils.convertScanner(input, tempTable.getTempTableName(),hBaseClient);
                }
            },keyDistributor,
                    new Comparator<byte[]>() {
                        @Override
                        public int compare(byte[] o1, byte[] o2) {
                            /*
                             * We want to ignore the operationUid and bucket id when sorting data. In this
                             * case, our data starts at location 10 (bucket + opUUid+0x00).length = 10
                             */
                            int prefixLength = 11;
                            return Bytes.compareTo(o1,prefixLength,o1.length-prefixLength,o2,prefixLength,o2.length-prefixLength);
                        }
                    });

            scanner.open();
            return new ForwardingCloseableIterator<Long>(Iterators.transform(scanner.iterator(),new Function<Result, Long>() {
                @Nullable
                @Override
                public Long apply(@Nullable Result input) {
                    byte[] row = input.getRow();
                    /*
                     * format = <bucket> <op uuid> 0x00 <txnId>
                     *
                     *     so txnid is at location 10
                     */
                    return Bytes.toLong(row,10);
                }
            })) {
                @Override
                public void close() throws IOException {
                    try{
                        scanner.close();
                    }finally{
                        try {
                            submit.cleanup();
                        } catch (ExecutionException e) {
                            throw new IOException(e.getCause());
                        }
                    }
                }
            };

        } catch (ExecutionException e) {
            throw Exceptions.getIOException(e);
        } catch (InterruptedException e) {
            throw Exceptions.getIOException(e);
        }
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

        private ActiveTxnJob(byte[] operationUUID, int queueSize) {
            this.operationUUID = operationUUID;
            this.queueSize = queueSize;
        }

        @Override
        public String getJobId() {
            return String.format("ActiveTransactions-%d", Bytes.toLong(operationUUID));
        }

        @Override
        public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
            ActiveTransactionTask task = new ActiveTransactionTask(getJobId(), minTxnId, maxTxnId, writeTable, operationUUID,queueSize);
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
