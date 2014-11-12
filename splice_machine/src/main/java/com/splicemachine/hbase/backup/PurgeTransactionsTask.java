package com.splicemachine.hbase.backup;

import com.splicemachine.derby.impl.job.ZkTask;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.SchedulerPriorities;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *
 *
 */
public class PurgeTransactionsTask extends ZkTask {
    private static final long serialVersionUID = 5l;
    private long backupTimestamp;

    public PurgeTransactionsTask() {
    }

    public PurgeTransactionsTask(long backupTimestamp, String jobId) {
        super(jobId, OperationJob.operationTaskPriority);
        this.backupTimestamp = backupTimestamp;
    }

    @Override
    protected String getTaskType() {
        return "purgeTransactionsTask";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeLong(backupTimestamp); // TODO Needs to be replaced with protobuf
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        backupTimestamp = in.readLong(); // TODO Needs to be replaced with protobuf
    }

    @Override
    public boolean invalidateOnClose() {
        return true;
    }

    @Override
    public RegionTask getClone() {
        return new PurgeTransactionsTask(backupTimestamp, jobId);
    }

    @Override
    public void doExecute() throws ExecutionException, InterruptedException {
        try {
            RegionTxnPurger txnPurger = new RegionTxnPurger(region);
            txnPurger.rollbackTransactionsAfter(backupTimestamp);
        } catch (IOException e) {
            SpliceLogUtils.error(LOG, "Couldn't purge transactions from region " + region, e);
            throw new ExecutionException("Failed purge of transactions of region " + region, e);
        }
    }

    @Override
    public int getPriority() {
        return SchedulerPriorities.INSTANCE.getBasePriority(PurgeTransactionsTask.class);
    }
}

