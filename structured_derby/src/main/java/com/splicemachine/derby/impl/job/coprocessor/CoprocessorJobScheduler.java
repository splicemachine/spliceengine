package com.splicemachine.derby.impl.job.coprocessor;

import com.splicemachine.derby.impl.job.OperationJob;
import com.splicemachine.derby.impl.job.scheduler.ZkBackedJobScheduler;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/4/13
 */
public class CoprocessorJobScheduler extends ZkBackedJobScheduler<OperationJob>{
    private static final Logger LOG = Logger.getLogger(CoprocessorJobScheduler.class);
    public CoprocessorJobScheduler(RecoverableZooKeeper zooKeeper) {
        super(zooKeeper);
    }

    @Override
    protected Set<WatchingTask> submitTasks(final OperationJob job) throws ExecutionException {
        SpliceLogUtils.trace(LOG,"submitting job %s",job.getJobId());
        Scan scan = job.getScan();
        HTableInterface table = job.getTable();
        try {
            final Set<WatchingTask> tasks = Collections.newSetFromMap(new ConcurrentHashMap<WatchingTask, Boolean>());
            SpliceLogUtils.trace(LOG,"executing coprocessor submission for job %s",job.getJobId());
            table.coprocessorExec(SpliceSchedulerProtocol.class,scan.getStartRow(),scan.getStartRow(),new Batch.Call<SpliceSchedulerProtocol, TaskFutureContext>() {
                @Override
                public TaskFutureContext call(SpliceSchedulerProtocol instance) throws IOException {
                    return instance.submit(new SinkTask(job.getScan(),job.getInstructions()));
                }
            },new Batch.Callback<TaskFutureContext>() {
                        @Override
                        public void update(byte[] region, byte[] row, TaskFutureContext result) {
                            //TODO -sf- deal with invalidated tasks that require resubmission
                            tasks.add(new WatchingTask(result,zooKeeper));
                        }
                    });
            return tasks;
        } catch (Throwable throwable) {
            SpliceLogUtils.error(LOG,"Encountered error submitting job to coprocessor",throwable);
            throw new ExecutionException(Exceptions.parseException(throwable));
        }
    }

}
