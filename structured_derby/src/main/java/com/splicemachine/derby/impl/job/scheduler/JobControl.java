package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.collect.Lists;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.coprocessor.SpliceSchedulerProtocol;
import com.splicemachine.derby.impl.job.coprocessor.TaskFutureContext;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.AttemptsExhaustedException;
import com.splicemachine.hbase.table.BoundCall;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobStats;
import com.splicemachine.job.Status;
import com.splicemachine.job.TaskFuture;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.apache.zookeeper.AsyncCallback;
import org.apache.zookeeper.ZooKeeper;

import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Created on: 9/17/13
 */
class JobControl implements JobFuture {
    private static final Logger LOG = Logger.getLogger(JobControl.class);
    private final CoprocessorJob job;
    private final NavigableSet<RegionTaskControl> tasksToWatch;
    private final BlockingQueue<RegionTaskControl> changedTasks;
    private final Set<RegionTaskControl> failedTasks;
    private final Set<RegionTaskControl> completedTasks;
    private final Set<RegionTaskControl> cancelledTasks;
    private final JobStatsAccumulator stats;
    private final SpliceZooKeeperManager zkManager;
    private final int maxResubmissionAttempts;
    private final JobMetrics jobMetrics;
    private final String jobPath;
    private final LinkedList<Closeable> closables;
    private volatile boolean cancelled = false;

    JobControl(CoprocessorJob job, String jobPath,SpliceZooKeeperManager zkManager, int maxResubmissionAttempts, JobMetrics jobMetrics){
        this.job = job;
        this.jobPath = jobPath;
        this.zkManager = zkManager;
        this.jobMetrics = jobMetrics;
        this.stats = new JobStatsAccumulator(job.getJobId());
        this.tasksToWatch = new ConcurrentSkipListSet<RegionTaskControl>();

        this.changedTasks = new LinkedBlockingQueue<RegionTaskControl>();
        this.failedTasks = Collections.newSetFromMap(new ConcurrentHashMap<RegionTaskControl, Boolean>());
        this.completedTasks = Collections.newSetFromMap(new ConcurrentHashMap<RegionTaskControl, Boolean>());
        this.cancelledTasks = Collections.newSetFromMap(new ConcurrentHashMap<RegionTaskControl, Boolean>());
        this.closables = Lists.newLinkedList();
        this.maxResubmissionAttempts = maxResubmissionAttempts;
    }

    @Override
    public Status getStatus() throws ExecutionException {
        if(failedTasks.size()>0) return Status.FAILED;
        else if(cancelled) return Status.CANCELLED;
        else if(completedTasks.size()>=tasksToWatch.size()) return Status.COMPLETED;
        else return Status.EXECUTING;
    }

    @Override
    public void completeAll(StatusHook statusHook) throws ExecutionException, InterruptedException, CancellationException {
        while(getRemainingTasks()>0)
            completeNext(statusHook);
    }

    @Override
    public void completeNext(StatusHook statusHook) throws ExecutionException, InterruptedException, CancellationException {
        if (failedTasks.size() > 0) {
            for (RegionTaskControl taskControl : failedTasks)
                taskControl.complete(); //throw the error right away
        } else if (cancelled)
            throw new CancellationException();

        RegionTaskControl changedFuture;
        int futuresRemaining = getRemainingTasks();
        SpliceLogUtils.trace(LOG, "[%s]Tasks remaining: %d", job.getJobId(), futuresRemaining);
        boolean found;
        while (futuresRemaining > 0) {
            changedFuture = changedTasks.take();
            if (cancelled)
                throw new CancellationException();
            found = !completedTasks.contains(changedFuture) &&
                    !failedTasks.contains(changedFuture) &&
                    !cancelledTasks.contains(changedFuture);

            futuresRemaining = getRemainingTasks();
            if (!found) continue;

            Status status = changedFuture.getStatus();
            switch (status) {
                case INVALID:
                    changedFuture.cleanup();
                    SpliceLogUtils.trace(LOG, "[%s] Task %s is invalid, resubmitting", job.getJobId(), changedFuture.getTaskNode());
                    stats.invalidTaskCount.incrementAndGet();
                    if (statusHook != null)
                        statusHook.invalidated(changedFuture.getTaskId());

                    if (changedFuture.rollback(maxResubmissionAttempts)) {
                        resubmit(changedFuture, changedFuture.tryNumber());
                    } else {
                        //we were unable to roll back, so we have to bomb out
                        failedTasks.add(changedFuture);
                        completeNext(statusHook); //throw the proper error
                    }
                    break;
                case FAILED:
                    if (statusHook != null)
                        statusHook.failure(changedFuture.getTaskId());
                    changedFuture.cleanup();
                    try {
                        SpliceLogUtils.trace(LOG, "[%s] Task %s failed", job.getJobId(), changedFuture.getTaskNode());
                        stats.addFailedTask(changedFuture.getTaskId());
                        changedFuture.dealWithError();
                    } catch (ExecutionException ee) {
                        //update our metrics
                        failedTasks.add(changedFuture);
                        throw ee;
                    }
                    break;
                case COMPLETED:
                    SpliceLogUtils.trace(LOG, "[%s] Task %s completed successfully", job.getJobId(), changedFuture.getTaskNode());
                    changedFuture.cleanup();
                    if (changedFuture.commit(maxResubmissionAttempts)) {
                        TaskStats taskStats = changedFuture.getTaskStats();
                        if (taskStats != null)
                            this.stats.addTaskStatus(changedFuture.getTaskNode(), taskStats);
                        completedTasks.add(changedFuture);
                        if (statusHook != null)
                            statusHook.success(changedFuture.getTaskId());
                        return;
                    } else {
                        //our commit failed, we have to resubmit the task (if possible)
                        SpliceLogUtils.debug(LOG, "[%s] Task %s did not successfully commit", job.getJobId(), changedFuture.getTaskNode());
                        if (statusHook != null)
                            statusHook.failure(changedFuture.getTaskId());
                        changedFuture.dealWithError();
                    }
                    break;
                case CANCELLED:
                    SpliceLogUtils.trace(LOG, "[%s] Task %s is cancelled", job.getJobId(), changedFuture.getTaskNode());
                    if (statusHook != null)
                        statusHook.cancelled(changedFuture.getTaskId());
                    changedFuture.cleanup();
										changedFuture.rollback(maxResubmissionAttempts);
                    cancelledTasks.add(changedFuture);
                    throw new CancellationException();
                default:
                    SpliceLogUtils.trace(LOG, "[%s] Task %s is in state %s", job.getJobId(), changedFuture.getTaskNode(), status);
            }
        }

//        //update our job metrics
//        Status finalStatus = status();
//        jobMetrics.removeJob(job.getJobId(), finalStatus);
//
        SpliceLogUtils.trace(LOG, "completeNext finished");
    }

    @Override
    public void cancel() throws ExecutionException {
				cancelled=true;
				cleanup();
    }

    @Override
    public double getEstimatedCost() throws ExecutionException {
        double maxCost = 0d;
        for(TaskFuture future:tasksToWatch){
            if(maxCost < future.getEstimatedCost()){
                maxCost = future.getEstimatedCost();
            }
        }
        return maxCost;
    }

    @Override
    public void cleanup() throws ExecutionException {
        SpliceLogUtils.trace(LOG, "cleaning up job %s", job.getJobId());
        try {
            ZooKeeper zooKeeper = zkManager.getRecoverableZooKeeper().getZooKeeper();
            zooKeeper.delete(jobPath, -1, new AsyncCallback.VoidCallback() {
                @Override
                public void processResult(int i, String s, Object o) {
                    LOG.trace("Result for deleting path " + jobPath + ": i=" + i + ", s=" + s);
                }
            }, this);
            for (RegionTaskControl task : tasksToWatch) {
                zooKeeper.delete(task.getTaskNode(), -1, new AsyncCallback.VoidCallback() {
                    @Override
                    public void processResult(int i, String s, Object o) {
                        LOG.trace("Result for deleting path " + jobPath + ": i=" + i + ", s=" + s);
                    }
                }, this);
            }
        } catch (ZooKeeperConnectionException e) {
            throw new ExecutionException(e);
        } finally {
            try {
                for (Closeable c : closables) {
                    c.close();
                }
            } catch (IOException ioe) {
                throw new ExecutionException(ioe);
            } finally {
                closables.clear();
                completedTasks.clear();
                failedTasks.clear();
                tasksToWatch.clear();
                changedTasks.clear();
            }
        }
    }

    @Override
    public void addCleanupTask(Closeable closable) {
        // prepend, so closables are closed in reverse
        closables.add(0, closable);
    }

    @Override public JobStats getJobStats() { return stats; }
    @Override public int getNumTasks() { return tasksToWatch.size(); }

    @Override
    public int getRemainingTasks() {
        return tasksToWatch.size()-completedTasks.size()-failedTasks.size()-cancelledTasks.size();
    }

		@Override
		public byte[][] getAllTaskIds() {
				byte[][] tIds = new byte[tasksToWatch.size()][];
				int i=0;
				for(RegionTaskControl taskControl:tasksToWatch){
						tIds[i] = taskControl.getTaskId();
						i++;
				}
				return tIds;
		}

		/*************************************************************************************************************************/
    /*package local operators*/

    /*
     * Notify the JobControl that an individual task has changed
     */
    void taskChanged(RegionTaskControl taskControl){
        this.changedTasks.add(taskControl);
    }

    /*
     * Notify the JobControl that an individual task has been invalidated
     */
    @SuppressWarnings("UnusedParameters")
    void markInvalid(RegionTaskControl regionTaskControl) {
        stats.invalidTaskCount.incrementAndGet();
    }

    /*
     * Resubmit a task.
     *
     * if tryCount >=maxResubmissionAttempts, then we cannot attempt this task any longer. An AttemptsExhaustedException
     * will be thrown
     */
    void resubmit(RegionTaskControl task,
                  int tryCount) throws ExecutionException {
        //only submit so many times
        if(tryCount>=maxResubmissionAttempts){
            Throwable lastError = task.getError();
            if(lastError!=null)
                throw new ExecutionException(lastError);

            //we don't know what went wrong, so blow up with an AttemptsExhausted
            ExecutionException ee = new ExecutionException(
                    new AttemptsExhaustedException("Unable to complete task "+ task.getTaskNode()+", it was invalidated more than "+ maxResubmissionAttempts+" times"));
            task.fail(ee.getCause());
            throw ee;
        }

        //get the next higher task
        RegionTaskControl next = task;
        do{
            next = tasksToWatch.higher(next);
            /*
             * Can't do a direct compareTo() call, because the compareTo() method must
             * distinguish between two regions which have the same start/stop key but are
             * distinct task nodes, where here we want to treat those the same.
             */
        }while(next!=null && Bytes.compareTo(next.getStartRow(),task.getStartRow())==0);

        tasksToWatch.remove(task);

        byte[] endRow;
        byte[] start = task.getStartRow();
        if(next!=null){
            byte[] nextStart = next.getStartRow();
            endRow = new byte[nextStart.length];
            System.arraycopy(nextStart,0,endRow,0,endRow.length);

            BytesUtil.unsignedDecrement(endRow, endRow.length - 1);
        }else
            endRow = HConstants.EMPTY_END_ROW;

        try{
            Pair<RegionTask,Pair<byte[],byte[]>> newTaskData = job.resubmitTask(task.getTask(), start, endRow);
            if (LOG.isTraceEnabled())
                SpliceLogUtils.trace(LOG, "executing submit on resubmitted job %s", job.getJobId());

            //submit the task again
            submit(newTaskData.getFirst(), newTaskData.getSecond(), job.getTable(), tryCount + 1);


        }catch(IOException ioe){
            throw new ExecutionException(ioe);
        }
    }

    /*
     * Physically submits a Task.
     */
    void submit(final RegionTask task,
                        Pair<byte[], byte[]> range,
                        HTableInterface table,
                        final int tryCount) throws ExecutionException {
        final byte[] start = range.getFirst();
        final byte[] stop = range.getSecond();

        try{
            table.coprocessorExec(SpliceSchedulerProtocol.class,start,stop,
                    new BoundCall<SpliceSchedulerProtocol, TaskFutureContext>() {
                        @Override
                        public TaskFutureContext call(SpliceSchedulerProtocol instance) throws IOException {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public TaskFutureContext call(byte[] startKey, byte[] stopKey, SpliceSchedulerProtocol instance) throws IOException {
                            return instance.submit(startKey,stopKey,task);
                        }
                    }, new Batch.Callback<TaskFutureContext>() {
                        @Override
                        public void update(byte[] region, byte[] row, TaskFutureContext result) {
                            RegionTaskControl control = new RegionTaskControl(row,task,JobControl.this,result,tryCount,zkManager);
                            tasksToWatch.add(control);
                            taskChanged(control);
                        }
                    });
            jobMetrics.addJob(job);
        }catch (Throwable throwable) {
            throw new ExecutionException(throwable);
        }
    }
}
