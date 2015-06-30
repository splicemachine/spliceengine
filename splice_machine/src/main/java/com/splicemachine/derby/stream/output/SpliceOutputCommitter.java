package com.splicemachine.derby.stream.output;

import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.impl.TransactionLifecycle;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by jleach on 5/18/15.
 */
public class SpliceOutputCommitter extends OutputCommitter {
    private static Logger LOG = Logger.getLogger(SpliceOutputCommitter.class);
    protected TxnView parentTxn;
    protected byte[] destinationTable;
    protected Map<TaskAttemptID,TxnView> taskAttemptMap = new HashMap<TaskAttemptID,TxnView>();

    private SpliceOutputCommitter() {
        super();
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"init");
    }

    public SpliceOutputCommitter(TxnView parentTxn, byte[] destinationTable) {
        super();
        this.parentTxn = parentTxn;
        this.destinationTable = destinationTable;
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"init with txn=%s and destinationTable=%s",parentTxn,destinationTable);
    }

    @Override
    public void setupJob(org.apache.hadoop.mapreduce.JobContext jobContext) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"setupJob");
    }

    @Override
    public void cleanupJob(org.apache.hadoop.mapreduce.JobContext jobContext) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"cleanup Job");
    }

    @Override
    public void commitJob(org.apache.hadoop.mapreduce.JobContext jobContext) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"commit Job");
    }

    @Override
    public void abortJob(org.apache.hadoop.mapreduce.JobContext jobContext, JobStatus.State state) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"abort Job");
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"setupTask");
        TxnView childTxn = TransactionLifecycle.getLifecycleManager().beginChildTransaction(parentTxn,destinationTable);
        taskAttemptMap.put(taskContext.getTaskAttemptID(),childTxn);
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"beginTxn=%s and destinationTable=%s",childTxn,destinationTable);
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
        return true;
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) throws IOException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"commitTask");
        TxnView txn = taskAttemptMap.remove(taskContext.getTaskAttemptID());
        if (txn == null)
            throw new IOException("no transaction associated with task attempt Id "+taskContext.getTaskAttemptID());
        TransactionLifecycle.getLifecycleManager().commit(txn.getTxnId());
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG,"commitTxn=%s and destinationTable=%s",txn,destinationTable);
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) throws IOException {
        TxnView txn = taskAttemptMap.get(taskContext.getTaskAttemptID());
        if (txn == null)
            throw new IOException("no transaction associated with task attempt Id "+taskContext.getTaskAttemptID());
        TransactionLifecycle.getLifecycleManager().rollback(txn.getTxnId());
    }

    @Override
    public boolean isRecoverySupported() {
        return false;
    }

    public boolean isRecoverySupported(org.apache.hadoop.mapreduce.JobContext jobContext) throws IOException {
        return false;
    }

    @Override
    public void recoverTask(TaskAttemptContext taskContext) throws IOException {
        super.recoverTask(taskContext);
    }

    public TxnView getChildTransaction(TaskAttemptID taskAttemptID) {
        return taskAttemptMap.get(taskAttemptID);
    }

}
