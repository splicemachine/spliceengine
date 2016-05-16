package com.splicemachine.derby.stream.spark.fake;

import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.stream.output.SpliceOutputCommitter;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.TaskAttemptID;

import java.io.File;
import java.io.IOException;

public class FakeOutputCommitter extends SpliceOutputCommitter {

    public FakeOutputCommitter() {
        super(Txn.ROOT_TRANSACTION, new byte[0]);
    }

    @Override
    public TxnView getChildTransaction(TaskAttemptID taskAttemptID) {
        return parentTxn;
    }

    @Override
    public void setupJob(JobContext jobContext) throws IOException {

    }

    @Override
    public void setupTask(TaskAttemptContext taskAttemptContext) throws IOException {

    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskAttemptContext) throws IOException {
        return true;
    }

    @Override
    public void commitTask(TaskAttemptContext taskAttemptContext) throws IOException {

    }

    @Override
    public void abortTask(TaskAttemptContext taskAttemptContext) throws IOException {
        String abortDirectory = taskAttemptContext.getConfiguration().get("abort.directory");
        File file = new File(abortDirectory, taskAttemptContext.getTaskAttemptID().getTaskID().toString());
        file.createNewFile();
    }
}