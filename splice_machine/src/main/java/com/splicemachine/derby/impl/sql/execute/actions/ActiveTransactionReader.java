package com.splicemachine.derby.impl.sql.execute.actions;

import com.splicemachine.async.*;
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
import com.splicemachine.derby.utils.marshall.BucketHasher;
import com.splicemachine.hbase.FilteredRowKeyDistributor;
import com.splicemachine.hbase.RowKeyDistributor;
import com.splicemachine.hbase.RowKeyDistributorByHashPrefix;
import com.splicemachine.hbase.ScanDivider;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobStats;
import com.splicemachine.job.Task;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.si.api.SIFactory;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.stream.Stream;
import com.splicemachine.stream.StreamException;
import com.splicemachine.stream.Transformer;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Date: 7/30/14
 */
public class ActiveTransactionReader {
    private final long minTxnId;
    private final long maxTxnId;
    private final byte[] writeTable;
    private static final SIFactory siFactory = SIFactoryDriver.getSIFactory();

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
        return runJobAndScan(queueSize, job);
    }

    public Stream<TxnView> getActiveTransactions(int queueSize) throws IOException{
        byte[] operationId = SpliceDriver.driver().getUUIDGenerator().nextUUIDBytes();
        ActiveTxnJob job = new ActiveTxnJob(operationId, queueSize);
        return runJobAndScan(queueSize, job);
    }

    protected Stream<TxnView> runJobAndScan(int queueSize, ActiveTxnJob job) throws IOException {
        try {
            final JobFuture submit = SpliceDriver.driver().getJobScheduler().submit(job);
            submit.completeAll(null);
            JobStats jobStats = submit.getJobStats();

            boolean[] usedTempBuckets = getUsedTempBuckets(jobStats);
            final AsyncScanner scanner = configureResultScan(queueSize, job.operationUUID, usedTempBuckets, submit);
            return scanner.stream().transform(new Transformer<List<KeyValue>, TxnView>() {
                @Override
                public TxnView transform(List<KeyValue> element) throws StreamException {
                	return siFactory.transform(element);
                }
            });
        } catch (ExecutionException | InterruptedException e) {
            throw Exceptions.getIOException(e);
        }
    }

    /*private helper methods*/
    
    private AsyncScanner configureResultScan(int queueSize, byte[] operationId, boolean[] usedTempBuckets, final JobFuture submit) throws IOException {
        final TempTable tempTable = SpliceDriver.driver().getTempTable();
        RowKeyDistributor keyDistributor = new RowKeyDistributorByHashPrefix(BucketHasher.getHasher(tempTable.getCurrentSpread()));
        keyDistributor = new FilteredRowKeyDistributor(keyDistributor,usedTempBuckets);

        Scan scan = new Scan();
        scan.setStartRow(operationId);
        scan.setStopRow(BytesUtil.unsignedCopyAndIncrement(operationId));
//        Function<Scan, Scanner> convertFunction = DerbyAsyncScannerUtils.convertFunction(tempTable.getTempTableName(), AsyncHbase.HBASE_CLIENT);
        List<Scan> dividedScans = ScanDivider.divide(scan, keyDistributor);
        List<Scanner> scanners = DerbyAsyncScannerUtils.convertScanners(dividedScans,tempTable.getTempTableName(),AsyncHbase.HBASE_CLIENT,false);

        final AsyncScanner scanner = new SortedMultiScanner(
                scanners,
                queueSize,
                new Comparator<byte[]>() {
                    @Override
                    public int compare(byte[] o1, byte[] o2) {
                                /*
                                 * We want to ignore the operationUid and bucket id when sorting data. In this
                                 * case, our data starts at location 10 (bucket + opUUid+0x00).length = 10
                                 */
                        int prefixLength = 10;
                        return Bytes.compareTo(o1, prefixLength, o1.length - prefixLength, o2, prefixLength, o2.length - prefixLength);
                    }
                },
                Metrics.noOpMetricFactory()){
            @Override
            public void close() {
                try {
                    super.close();
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

        scanner.open();
        return scanner;
    }

    /**
     * Callable to pass to scanner construction so that when the scanner closes,
     * the active transaction job also gets cleaned up.
     */
    private static class JobCleanup implements Callable<Void> {
    	private JobFuture future;

        private JobCleanup(JobFuture jobFuture) {
    		future = jobFuture;
    	}
    	
    	public Void call() throws Exception {
    		future.cleanup();
    		return null;
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
