package com.splicemachine.derby.impl.sql.execute.actions;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.storage.DerbyAsyncScannerUtils;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.marshall.BucketHasher;
import com.splicemachine.hbase.FilteredRowKeyDistributor;
import com.splicemachine.hbase.RowKeyDistributor;
import com.splicemachine.hbase.RowKeyDistributorByHashPrefix;
import com.splicemachine.hbase.async.AsyncScanner;
import com.splicemachine.hbase.async.SimpleAsyncScanner;
import com.splicemachine.hbase.async.SortedGatheringScanner;
import com.splicemachine.hbase.table.SpliceHTableFactory;
import com.splicemachine.job.Job;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobStats;
import com.splicemachine.job.Task;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.si.impl.TransactionStore;
import com.splicemachine.stats.Metrics;
import com.splicemachine.utils.CloseableIterator;
import com.splicemachine.utils.ForwardingCloseableIterator;
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
        byte[] operationId = SpliceDriver.driver().getUUIDGenerator().nextUUIDBytes();
        try {
            JobFuture submit = SpliceDriver.driver().getJobScheduler().submit(new ActiveTxnJob(operationId));
            submit.completeAll(null);
            JobStats jobStats = submit.getJobStats();

            boolean[] usedTempBuckets = getUsedTempBuckets(jobStats);
            RowKeyDistributor keyDistributor = new RowKeyDistributorByHashPrefix(BucketHasher.getHasher(SpliceDriver.driver().getTempTable().getCurrentSpread()));
            keyDistributor = new FilteredRowKeyDistributor(keyDistributor,usedTempBuckets);

            Scan scan = new Scan();
            scan.setStartRow(operationId);
            byte[] stop = new byte[operationId.length+1];
            System.arraycopy(operationId,0,stop,0,operationId.length);
            stop[stop.length-1] = 0x01;
            scan.setStopRow(stop);
            final HBaseClient hBaseClient = SimpleAsyncScanner.HBASE_CLIENT;
            final AsyncScanner scanner = SortedGatheringScanner.newScanner(scan,4096, Metrics.noOpMetricFactory(), new Function<Scan, Scanner>() {
                @Nullable
                @Override
                public Scanner apply(@Nullable Scan input) {
                    return DerbyAsyncScannerUtils.convertScanner(input,SIConstants.TRANSACTION_TABLE_BYTES,hBaseClient);
                }
            },keyDistributor);

            return new ForwardingCloseableIterator<Long>(Iterators.transform(scanner.iterator(),new Function<Result, Long>() {
                @Nullable
                @Override
                public Long apply(@Nullable Result input) {
                    byte[] row = input.getRow();
                    /*
                     * format = <bucket> 0x00 <op uuid> 0x00 <txnId>
                     *
                     *     so txnid is at location 11
                     */
                    return Bytes.toLong(row,11);
                }
            })) {
                @Override
                public void close() throws IOException {
                    scanner.close();
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

        private ActiveTxnJob(byte[] operationUUID) {
            this.operationUUID = operationUUID;
        }

        @Override
        public String getJobId() {
            return String.format("ActiveTransactions-%d", Bytes.toLong(operationUUID));
        }

        @Override
        public Map<? extends RegionTask, Pair<byte[], byte[]>> getTasks() throws Exception {
            return Collections.singletonMap(new ActiveTransactionTask(getJobId(),minTxnId,maxTxnId,writeTable,operationUUID),Pair.newPair(HConstants.EMPTY_START_ROW,HConstants.EMPTY_END_ROW));
        }

        @Override
        public HTableInterface getTable() {
            return SpliceAccessManager.getHTable(SIConstants.TRANSACTION_TABLE_BYTES);
        }

        @Override public TransactionId getParentTransaction() { return null; }
        @Override public boolean isReadOnly() { return true; }

        @Override
        public <T extends Task> Pair<T, Pair<byte[], byte[]>> resubmitTask(T originalTask, byte[] taskStartKey, byte[] taskEndKey) throws IOException {
            return Pair.newPair(originalTask,Pair.newPair(taskStartKey,taskEndKey));
        }
    }
}
