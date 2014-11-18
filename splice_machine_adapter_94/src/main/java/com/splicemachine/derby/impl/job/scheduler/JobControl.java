package com.splicemachine.derby.impl.job.scheduler;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.coprocessor.SpliceSchedulerProtocol;
import com.splicemachine.derby.impl.job.coprocessor.TaskFutureContext;
import com.splicemachine.hbase.table.BoundCall;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Created on: 9/17/13
 */
public class JobControl extends BaseJobControl {
    private static final Logger LOG = Logger.getLogger(JobControl.class);

    public JobControl(CoprocessorJob job, String jobPath,SpliceZooKeeperManager zkManager, int maxResubmissionAttempts, JobMetrics jobMetrics){
    	super(job,jobPath,zkManager,maxResubmissionAttempts,jobMetrics);
    }
    /*
     * Physically submits a Task.
     */
    public void submit(final RegionTask task,
                        Pair<byte[], byte[]> range,
                        HTableInterface table,
                        final int tryCount) throws ExecutionException {
        final byte[] start = range.getFirst();
        final byte[] stop = range.getSecond();

        try{
            table.coprocessorExec(SpliceSchedulerProtocol.class, start, stop,
                    new BoundCall<SpliceSchedulerProtocol, TaskFutureContext[]>() {
                        @Override
                        public TaskFutureContext[] call(SpliceSchedulerProtocol instance) throws IOException {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public TaskFutureContext[] call(byte[] startKey, byte[] stopKey, SpliceSchedulerProtocol instance) throws IOException {
                            return instance.submit(startKey, stopKey, task, tryCount == 0); //only allow splitting the first time
                        }
                    }, new Batch.Callback<TaskFutureContext[]>() {
                        @Override
                        public void update(byte[] region, byte[] row, TaskFutureContext[] results) {
                            for (TaskFutureContext result : results) {
                                RegionTaskControl control = new RegionTaskControl(result.getStartRow(), task, JobControl.this, result, tryCount, zkManager);
                                tasksToWatch.add(control);
                                taskChanged(control);
                            }
                        }
                    }
            );
        }catch (Throwable throwable) {
            throw new ExecutionException(throwable);
        }
    }
}
