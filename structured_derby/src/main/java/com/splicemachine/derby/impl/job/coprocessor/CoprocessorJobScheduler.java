package com.splicemachine.derby.impl.job.coprocessor;

import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.job.scheduler.ZkBackedJobScheduler;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.job.Status;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 * Created on: 4/4/13
 */
public class CoprocessorJobScheduler extends ZkBackedJobScheduler<CoprocessorJob>{
    private static final Logger LOG = Logger.getLogger(CoprocessorJobScheduler.class);
    private static final int DEFAULT_MAX_RESUBMISSIONS = 3;
    private final int maxResubmissionAttempts;

    public CoprocessorJobScheduler(RecoverableZooKeeper zooKeeper,Configuration configuration) {
        super(zooKeeper);

        maxResubmissionAttempts = configuration.getInt("splice.task.maxResubmissions",DEFAULT_MAX_RESUBMISSIONS);
    }

    @Override
    protected Set<? extends WatchingTask> submitTasks(final CoprocessorJob job) throws ExecutionException {
        SpliceLogUtils.trace(LOG,"submitting job %s",job.getJobId());

        try {
            final NavigableSet<RegionWatchingTask> tasksToWatch = new ConcurrentSkipListSet<RegionWatchingTask>();
            Map<? extends RegionTask,Pair<byte[],byte[]>> tasks = job.getTasks();
            for(RegionTask task:tasks.keySet()){
                Pair<byte[],byte[]> range = tasks.get(task);
                byte[] start = range.getFirst();
                byte[] stop = range.getSecond();
                submitTask(task, start, stop, job.getTable(), tasksToWatch);
            }
            return tasksToWatch;
        } catch (Throwable throwable) {
            SpliceLogUtils.error(LOG,"Encountered error submitting job to coprocessor",throwable);
            throw new ExecutionException(Exceptions.parseException(throwable));
        }
    }

    private void submitTask(final RegionTask task,
                             byte[] startRow,
                             byte[] stopRow,
                             final HTableInterface table,
                             final NavigableSet<RegionWatchingTask> tasks) throws Throwable {
        table.coprocessorExec(SpliceSchedulerProtocol.class, startRow, stopRow, new Batch.Call<SpliceSchedulerProtocol, TaskFutureContext>() {
                    @Override
                    public TaskFutureContext call(SpliceSchedulerProtocol instance) throws IOException {
                        return instance.submit(task);
                    }
                }, new Batch.Callback<TaskFutureContext>() {
                    @Override
                    public void update(byte[] region, byte[] row, TaskFutureContext result) {
                        tasks.add(new RegionWatchingTask(result, zooKeeper, row, task, table, tasks));
                    }
                }
        );
    }

    private class RegionWatchingTask extends WatchingTask implements Comparable<RegionWatchingTask>{
        private final byte[] startRow;
        private final RegionTask task;
        private final NavigableSet<RegionWatchingTask> tasksToWatch;
        private final HTableInterface table;
        private final AtomicInteger submissionAttempts = new AtomicInteger(0);

        public RegionWatchingTask(TaskFutureContext result,
                                  RecoverableZooKeeper zooKeeper,
                                  byte[] startRow,
                                  RegionTask task,
                                  HTableInterface table,
                                  NavigableSet<RegionWatchingTask> taskSet ) {
            super(result,zooKeeper);
            this.startRow = startRow;
            this.task = task;
            this.tasksToWatch = taskSet;
            this.table = table;
        }


        @Override
        protected void manageInvalidated() throws ExecutionException {
            resubmitJob();
        }

        private void resubmitJob() throws ExecutionException {
            if(submissionAttempts.incrementAndGet()==maxResubmissionAttempts){
                //we've tried a bunch of times to run this task, and we just can't. That sucks
                //there's probably something wrong with the engine, so fail the task
                //and let the submitter know that we suck
                ExecutionException ee = new ExecutionException(new IOException("Unable to complete task, " +
                        "it was invalidated move than "+ maxResubmissionAttempts+" times"));
                status.setError(ee.getCause());
                status.setStatus(Status.FAILED);
                throw ee;
            }
            //get the next Watcher in the set higher than us, and find the new region
            //to submit the request against
            RegionWatchingTask nextTask = tasksToWatch.higher(this);
            //remove ourselves from the tasksToWatch list to make sure no one is listening to us
            tasksToWatch.remove(this);

            //make sure that we are wholly within the range needed to resubmit
            byte[] endRow = new byte[nextTask.startRow.length];
            System.arraycopy(nextTask.startRow,0,endRow,0,endRow.length);
            BytesUtil.decrementAtIndex(endRow,endRow.length-1);

            try {
                submitTask(task, startRow, endRow, table, tasksToWatch);
            } catch (Throwable throwable) {
                throw new ExecutionException(throwable);
            }

        }

        @Override
        public int compareTo(RegionWatchingTask o) {
            if(o==null) return 1;

            return Bytes.compareTo(startRow,o.startRow);
        }
    }
}
