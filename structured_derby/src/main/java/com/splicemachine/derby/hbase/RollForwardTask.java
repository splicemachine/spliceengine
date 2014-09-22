package com.splicemachine.derby.hbase;

import com.google.common.collect.Lists;
import com.google.common.io.Closeables;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.hbase.BufferedRegionScanner;
import com.splicemachine.hbase.MeasuredRegionScanner;
import com.splicemachine.job.Status;
import com.splicemachine.job.Task;
import com.splicemachine.job.TaskStatus;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.si.api.*;
import com.splicemachine.si.coprocessors.SIFilter;
import com.splicemachine.si.impl.DataStore;
import com.splicemachine.si.impl.SimpleTxnFilter;
import com.splicemachine.si.impl.TransactionalRegions;
import com.splicemachine.si.impl.TxnFilter;
import com.splicemachine.si.impl.readresolve.SynchronousReadResolver;
import com.splicemachine.si.impl.rollforward.SegmentedRollForward;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * Task-based structure for Rolling forward a range of data.
 *
 * @author Scott Fines
 * Date: 9/4/14
 */
public class RollForwardTask implements Task {
    private volatile boolean cancelled = false;
    private final HRegion region;
    private final byte[] start;
    private final byte[] stop;
    private final TaskStatus status;
    private final SegmentedRollForward.Context context;

    public RollForwardTask(HRegion region, byte[] start, byte[] stop, SegmentedRollForward.Context context) {
        this.region = region;
        this.start = start;
        this.stop = stop;
        this.context = context;
        this.status = new TaskStatus(Status.PENDING,null);
    }

    @Override public void markInvalid() throws ExecutionException {
        status.setStatus(Status.INVALID);
        cancelled =true;
    }
    @Override public void markStarted() throws ExecutionException, CancellationException {
        status.setStatus(Status.EXECUTING);
    }
    @Override public void markCompleted() throws ExecutionException {
        status.setStatus(Status.COMPLETED);
    }
    @Override public void markFailed(Throwable error) throws ExecutionException {
        status.setError(error);
        status.setStatus(Status.FAILED);
    }
    @Override public void markCancelled() throws ExecutionException { markCancelled(false); }
    @Override public void markCancelled(boolean propagate) throws ExecutionException {
        status.setStatus(Status.CANCELLED);
        cancelled = true;
    }

    @Override public boolean isCancelled() throws ExecutionException { return cancelled; }
    @Override public boolean isInvalidated() { return cancelled; }
    @Override public Txn getTxn() { return null; } //task is non-transactional
    @Override public byte[] getParentTaskId() { return null; }
    @Override public String getJobId() { return "rollForward-"+region.getRegionNameAsString(); }
    @Override public byte[] getTaskId() {
        return Bytes.toBytes(-1l); //
    }
    @Override public TaskStatus getTaskStatus() { return status; }

    @Override
    public void execute() throws ExecutionException, InterruptedException {
        try {
            Txn txn = TransactionLifecycle.getLifecycleManager().beginTransaction();
            MeasuredRegionScanner mrs = null;
            try{
                mrs = getRegionScanner(txn,context);
                List<KeyValue> kvs = Lists.newArrayList();
                int checkSize = 1024-1;
                int rowCount = 0;
                boolean shouldContinue;
                do{
                    if((rowCount &checkSize)==0){
                        SpliceBaseOperation.checkInterrupt();
                        //check for region closure
                        if(region.isClosed()||region.isClosing()){
                            break; //stop reading
                        }
                    }
                    shouldContinue = mrs.next(kvs);
                    if(kvs.size()<0) break;
                    rowCount++;

                }while(shouldContinue);
            }catch(Exception e){
                txn.rollback();
                throw e;
            }finally{
                context.complete();
                Closeables.closeQuietly(mrs);
            }
            txn.commit();
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    private MeasuredRegionScanner getRegionScanner(Txn txn,SegmentedRollForward.Context context) throws IOException {
        TxnSupplier txnSupplier = TransactionStorage.getTxnSupplier();
        //want to make sure that we bail the task on error
        ReadResolver resolver = SynchronousReadResolver.getResolver(region, txnSupplier, TransactionalRegions.getRollForwardStatus(),true);
        DataStore dataStore = TxnDataStore.getDataStore();
        TxnFilter filer = new UpdatingTxnFilter(txnSupplier,txn,resolver,dataStore,context);
        SIFilter filter = new SIFilter(filer);

        Scan scan = new Scan();
        scan.setStartRow(start);
        scan.setStopRow(stop);
        scan.setFilter(filter);
        scan.setCacheBlocks(false);

        return new BufferedRegionScanner(region,region.getScanner(scan), scan,16,Metrics.noOpMetricFactory());
    }

    @Override public void cleanup() throws ExecutionException {  }

    @Override
    public int getPriority() {
        return SchedulerPriorities.INSTANCE.getBasePriority(RollForwardTask.class);
    }

    private class UpdatingTxnFilter extends SimpleTxnFilter {
        private final SegmentedRollForward.Context context;

        public UpdatingTxnFilter(TxnSupplier transactionStore,
                                 TxnView myTxn,
                                 ReadResolver readResolver,
                                 DataStore dataStore,
                                 SegmentedRollForward.Context context) {
            super(transactionStore, myTxn, readResolver, dataStore);
            this.context = context;
        }

        @Override
        protected void doResolve(KeyValue keyValue, long ts) {
            super.doResolve(keyValue, ts);
            context.rowResolved();
        }
    }
}
