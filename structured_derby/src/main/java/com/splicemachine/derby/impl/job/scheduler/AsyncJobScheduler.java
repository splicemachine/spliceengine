package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorTaskScheduler;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.coprocessor.SpliceSchedulerProtocol;
import com.splicemachine.derby.impl.job.coprocessor.TaskFutureContext;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.job.*;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactorControl;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.ByteDataInput;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import com.splicemachine.utils.ZkUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 * Created on: 5/9/13
 */
public class AsyncJobScheduler implements JobScheduler<CoprocessorJob>,JobSchedulerManagement {
    private static final Logger LOG = Logger.getLogger(AsyncJobScheduler.class);
    private static final int DEFAULT_MAX_RESUBMISSIONS = 5;
    protected final SpliceZooKeeperManager zkManager;
    private final int maxResubmissionAttempts;

    private final AtomicLong totalSubmitted = new AtomicLong(0l);
    private final AtomicLong totalCompleted = new AtomicLong(0l);
    private final AtomicLong totalFailed = new AtomicLong(0l);
    private final AtomicLong totalCancelled = new AtomicLong(0l);
    private final AtomicInteger numRunning = new AtomicInteger(0);

    public AsyncJobScheduler(SpliceZooKeeperManager zkManager,Configuration configuration) {
        this.zkManager = zkManager;

        maxResubmissionAttempts = configuration.getInt("splice.tasks.maxResubmissions",DEFAULT_MAX_RESUBMISSIONS);
    }

    @Override
    public JobFuture submit(CoprocessorJob job) throws ExecutionException {
        totalSubmitted.incrementAndGet();
        try{
            String jobPath = createJobNode(job);
            return submitTasks(job);
        } catch (InterruptedException e) {
            throw new ExecutionException(e);
        } catch (KeeperException e) {
            throw new ExecutionException(e);
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    private JobFuture submitTasks(CoprocessorJob job) throws ExecutionException{
        SpliceLogUtils.trace(LOG,"submitting job %s",job.getJobId());
        final HTableInterface table = job.getTable();
        final Map<? extends RegionTask,Pair<byte[],byte[]>> tasks;
        try {
            tasks = job.getTasks();
        } catch (Exception e) {
            throw new ExecutionException(e);
        }

        Watcher watcher = new Watcher(job);
        for(final RegionTask task:tasks.keySet()){
            submit(watcher,job,task, tasks.get(task), table,0);
        }
        return watcher;
    }

    private String createJobNode(CoprocessorJob job) throws KeeperException, InterruptedException {
        String jobId = job.getJobId();
        jobId = jobId.replaceAll("/","_");
        String path = CoprocessorTaskScheduler.getJobPath()+"/"+jobId;
        ZkUtils.recursiveSafeCreate(path, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);

        return path;
    }

    @Override
    public long getTotalSubmittedJobs() {
        return totalSubmitted.get();
    }

    @Override
    public long getTotalCompletedJobs() {
        return totalCompleted.get();
    }

    @Override
    public long getTotalFailedJobs() {
        return totalFailed.get();
    }

    @Override
    public long getTotalCancelledJobs() {
        return totalCancelled.get();
    }

    @Override
    public int getNumRunningJobs() {
        return numRunning.get();
    }

    private void submit(final Watcher watcher,
                        final CoprocessorJob job,
                        final RegionTask task,
                        Pair<byte[],byte[]> range,
                        final HTableInterface table,
                        final int tryNum) throws ExecutionException{
        final byte[] start = range.getFirst();
        byte[] stop = range.getSecond();
        try {
            table.coprocessorExec(SpliceSchedulerProtocol.class, start
                    , stop, new Batch.Call<SpliceSchedulerProtocol, TaskFutureContext>() {
                        @Override
                        public TaskFutureContext call(SpliceSchedulerProtocol instance) throws IOException {
                            return instance.submit(task);
                        }
                    },new Batch.Callback<TaskFutureContext>() {
                        @Override
                        public void update(byte[] region, byte[] row, TaskFutureContext result) {
                            RegionTaskWatcher taskWatcher = new RegionTaskWatcher(watcher, row, job, task, result, table,tryNum);
                            watcher.tasksToWatch.add(taskWatcher);
                            watcher.taskChanged(taskWatcher);
                        }
                    });

        } catch (Throwable throwable) {
            throw new ExecutionException(throwable);
        }
    }

    /*
     * The meat of the Job Scheduler--the following two classes are responsible for
     *
     * 1. Managing the Transactional context of a task
     */
    private class RegionTaskWatcher implements Comparable<RegionTaskWatcher>,TaskFuture{
        private final byte[] startRow;
        private final RegionTask task;
        private final CoprocessorJob job;
        private final HTableInterface table;
        private final Watcher watcher;
        private final TaskFutureContext taskFutureContext;
        private volatile TaskStatus taskStatus = new TaskStatus(Status.PENDING,null);
        private volatile boolean refresh = true;
        private final int tryNum;

        private RegionTaskWatcher(Watcher watcher,
                                  byte[] startRow,
                                  CoprocessorJob job,
                                  RegionTask task,
                                  TaskFutureContext taskFutureContext,
                                  HTableInterface table,
                                  int tryNum) {
            this.watcher = watcher;
            this.job = job;
            this.startRow = startRow;
            this.task = task;
            this.table = table;
            this.taskFutureContext = taskFutureContext;
            this.tryNum = tryNum;
        }

        @Override
        public int compareTo(RegionTaskWatcher o) {
            if(o==null) return 1;
            int compare = Bytes.compareTo(startRow, o.startRow);
            if(compare!=0) return compare;

            //add this in to order tasks which go to the same node in some way
            return o.taskFutureContext.getTaskNode().compareTo(taskFutureContext.getTaskNode());
        }

        @Override
        public Status getStatus() throws ExecutionException {
            /*
             * These states are permanent--once entered they cannot be escaped, so there's
             * no point in refreshing even if an event is fired
             */
            switch (taskStatus.getStatus()) {
                case INVALID:
                case FAILED:
                case COMPLETED:
                case CANCELLED:
                    return taskStatus.getStatus();
            }
            /*
             * Unless we've received an event from ZooKeeper telling us the status has changed,
             * there's no need to fetch it. Thus, the watcher switches a refresh flag.
             */
            if(refresh){
                /*
                 * There is an inherent race-condition in this flag. Basically, what can happen is that
                 * the getData() call can return successfully (thus setting the watch), but before the
                 * old TaskStatus data has finished deserializing, the Watch is fired with a data changed
                 * situation. The Watch will set refresh = true, but if we set refresh = false at the end
                 * of this loop, then we can possibly override the setting in refresh that was set by the Watch.
                 * Thus, we reset the refresh flag here, and thus allow the watch to set the refresh flag without
                 * fear of being overriden.
                 */
                refresh=false;
                try{
                    byte[] data = zkManager.executeUnlessExpired(new SpliceZooKeeperManager.Command<byte[]>() {
                        @Override
                        public byte[] execute(RecoverableZooKeeper zooKeeper) throws InterruptedException, KeeperException {
                            try{
                                return zooKeeper.getData(
                                        taskFutureContext.getTaskNode(),
                                        new org.apache.zookeeper.Watcher() {
                                    @Override
                                    public void process(WatchedEvent event) {
                                        SpliceLogUtils.trace(LOG,"Received %s event on node %s",event.getType(),event.getPath());
                                        refresh=true;
                                        watcher.taskChanged(RegionTaskWatcher.this);
                                    }
                                },new Stat());
                            }catch(KeeperException ke){
                                if(ke.code()== KeeperException.Code.NONODE){
                                    /*
                                     * The Task status node was deleted. This happens in two situations:
                                     *
                                     * 1. Job cleanup
                                     * 2. Session Expiration of the Region operator.
                                     *
                                     * Since we are clearly not in the cleanup phase, then the server
                                     * responsible for executing this task has failed, so we need to
                                     * re-submit this task.
                                     *
                                     * The resubmission happens elsewhere, we just need to inform the
                                     * caller that we are in an INVALID state, and it'll re-submit. Assume
                                     * then that null = INVALID.
                                     */
                                    return null;
                                }
                                throw ke;
                            }
                        }
                    });
                    if(data==null){
                        /*
                         * Node failure, assume this = Status.INVALID
                         */
                        taskStatus.setStatus(Status.INVALID);
                    }else{
                        ByteDataInput bdi = new ByteDataInput(data);
                        taskStatus = (TaskStatus)bdi.readObject();
                    }
                } catch (InterruptedException e) {
                    throw new ExecutionException(e);
                } catch (KeeperException e) {
                    throw new ExecutionException(e);
                } catch (ClassNotFoundException e) {
                    throw new ExecutionException(e);
                } catch (IOException e) {
                    throw new ExecutionException(e);
                }
            }

            return taskStatus.getStatus();
        }

        @Override
        public void complete() throws ExecutionException, CancellationException, InterruptedException {
            while(true){
                Status runningStatus = getStatus();
                switch (runningStatus) {
                    case INVALID:
                        watcher.invalidCount.incrementAndGet();
                        resubmit();
                        break;
                    case FAILED:
                        dealWithError();
                        break;
                    case COMPLETED:
                        return;
                    case CANCELLED:
                        throw new CancellationException();
                }

                synchronized (taskFutureContext){
                    taskFutureContext.wait();
                }
            }
        }

        private void dealWithError() throws ExecutionException{
            if(taskStatus.shouldRetry())
                resubmit();
            else
                throw new ExecutionException(Exceptions.getWrapperException(taskStatus.getErrorMessage(),taskStatus.getErrorCode()));
        }

        private void resubmit() throws ExecutionException{
            SpliceLogUtils.debug(LOG,"resubmitting task %s",task.getTaskNode());
            //only submit so many time
            if(tryNum>=maxResubmissionAttempts){
                ExecutionException ee = new ExecutionException(
                        new RetriesExhaustedException("Unable to complete task "+ taskFutureContext.getTaskNode()+", it was invalidated more than "+ maxResubmissionAttempts+" times"));
                taskStatus.setError(ee.getCause());
                taskStatus.setStatus(Status.FAILED);
                throw ee;
            }

            RegionTaskWatcher nextTask = this;
            do{
                nextTask = watcher.tasksToWatch.higher(nextTask);
            }while(nextTask!=null && Bytes.compareTo(nextTask.startRow,startRow)==0); //skip past all tasks that are in the same position as me

            watcher.tasksToWatch.remove(this);

            //rollback child transaction
            if(taskStatus.getTransactionId()!=null){
                try {
                    final TransactorControl transactor = HTransactorFactory.getTransactorControl();
                    TransactionId txnId = transactor.transactionIdFromString(taskStatus.getTransactionId());
                    transactor.rollback(txnId);
                } catch (IOException e) {
                    Exception error = new DoNotRetryIOException("Unable to roll back child transaction",e);
                    taskStatus.setError(error);
                    taskStatus.setStatus(Status.FAILED);
                    throw new ExecutionException(error);
                }
            }

            byte[] endRow;
            if(nextTask!=null){
                endRow = new byte[nextTask.startRow.length];
                System.arraycopy(nextTask.startRow,0,endRow,0,endRow.length);

                BytesUtil.decrementAtIndex(endRow,endRow.length-1);
            }else
                endRow = HConstants.EMPTY_END_ROW;

            try{
                Pair<RegionTask,Pair<byte[],byte[]>> newTaskData = job.resubmitTask(task, startRow, endRow);

                //resubmit the task
                submit(watcher,job, newTaskData.getFirst(), newTaskData.getSecond(), table,tryNum+1);
            }catch(IOException ioe){
                throw new ExecutionException(ioe);
            }
        }

        @Override
        public double getEstimatedCost() {
            return taskFutureContext.getEstimatedCost();
        }

        @Override
        public byte[] getTaskId() {
            return taskFutureContext.getTaskId();
        }

        @Override
        public TaskStats getTaskStats() {
            return taskStatus.getStats();
        }

        public String getTransactionId() {
            return taskStatus.getTransactionId();
        }

        public String getTaskNode() {
            return taskFutureContext.getTaskNode();
        }
    }

    private static class JobStatsAccumulator implements JobStats{
        private Map<String,TaskStats> taskStatsMap = Maps.newConcurrentMap();
        private Watcher watcher;
        private List<String> failedTasks = Collections.synchronizedList(Lists.<String>newArrayListWithExpectedSize(0));

        private final AtomicInteger tasks = new AtomicInteger(0);
        private final AtomicInteger submittedTasks = new AtomicInteger();

        private final long start = System.nanoTime();

        private JobStatsAccumulator(Watcher watcher) {
            this.watcher = watcher;
        }

        @Override
        public int getNumTasks() {
            return tasks.get();
        }

        @Override
        public long getTotalTime() {
            return System.nanoTime()-start;
        }

        @Override
        public int getNumSubmittedTasks() {
            return submittedTasks.get();
        }

        @Override
        public int getNumCompletedTasks() {
            return watcher.completedTasks.size();
        }

        @Override
        public int getNumFailedTasks() {
            return failedTasks.size();
        }

        @Override
        public int getNumInvalidatedTasks() {
            return watcher.invalidCount.get();
        }

        @Override
        public int getNumCancelledTasks() {
            return watcher.cancelledTasks.size();
        }

        @Override
        public Map<String, TaskStats> getTaskStats() {
            return taskStatsMap;
        }

        @Override
        public String getJobName() {
            return watcher.job.getJobId();
        }

        @Override
        public List<String> getFailedTasks() {
            return failedTasks;
        }
    }

    private class Watcher implements JobFuture{
        private final CoprocessorJob job;
        private final NavigableSet<RegionTaskWatcher> tasksToWatch;
        private final BlockingQueue<RegionTaskWatcher> changedTasks;
        private final Set<RegionTaskWatcher> failedTasks;
        private final Set<RegionTaskWatcher> completedTasks;
        private final Set<RegionTaskWatcher> cancelledTasks;
        private volatile boolean cancelled = false;
        private JobStatsAccumulator stats;


        public AtomicInteger invalidCount = new AtomicInteger(0);

        private Watcher(CoprocessorJob job) {
            this.job = job;
            this.tasksToWatch = new ConcurrentSkipListSet<RegionTaskWatcher>();

            this.changedTasks = new LinkedBlockingQueue<RegionTaskWatcher>();
            this.failedTasks = Collections.newSetFromMap(new ConcurrentHashMap<RegionTaskWatcher, Boolean>());
            this.completedTasks = Collections.newSetFromMap(new ConcurrentHashMap<RegionTaskWatcher, Boolean>());
            this.cancelledTasks = Collections.newSetFromMap(new ConcurrentHashMap<RegionTaskWatcher, Boolean>());

            stats = new JobStatsAccumulator(this);
        }

        @Override
        public void cleanup() throws ExecutionException {
            SpliceLogUtils.trace(LOG,"cleaning up job %s",job.getJobId());
            try {
                zkManager.execute(new SpliceZooKeeperManager.Command<Void>() {
                    @Override
                    public Void execute(RecoverableZooKeeper zooKeeper) throws InterruptedException, KeeperException {
                        try{
                            zooKeeper.delete(CoprocessorTaskScheduler.getJobPath()+"/"+job.getJobId(),-1);
                        }catch(KeeperException ke){
                            if(ke.code()!= KeeperException.Code.NONODE)
                                throw ke;
                        }

                        for(RegionTaskWatcher task:tasksToWatch){
                            try{
                                zooKeeper.delete(task.getTaskNode(),-1);
                            }catch(KeeperException ke){
                                if(ke.code()!= KeeperException.Code.NONODE)
                                    throw ke;
                            }
                        }
                        return null;
                    }
                });
            } catch (InterruptedException e) {
                throw new ExecutionException(e);
            } catch (KeeperException e) {
                throw new ExecutionException(e);
            }
        }

        @Override
        public Status getStatus() throws ExecutionException {
            if(failedTasks.size()>0) return Status.FAILED;
            else if(cancelled) return Status.CANCELLED;
            else if(completedTasks.size()>=tasksToWatch.size()) return Status.COMPLETED;
            else return Status.EXECUTING;
        }

        @Override
        public void completeAll() throws ExecutionException, InterruptedException, CancellationException {
            int outStandingCount = getOutstandingCount();
            while(outStandingCount>0){
                SpliceLogUtils.trace(LOG,"%d tasks remaining",outStandingCount);
                completeNext();
                outStandingCount = getOutstandingCount();
            }

            numRunning.decrementAndGet();
        }

        @Override
        public void completeNext() throws ExecutionException, InterruptedException, CancellationException {
            if(failedTasks.size()>0){
                for(RegionTaskWatcher future:failedTasks) future.complete(); //throw an exception right away
            }else if(cancelled)
                throw new CancellationException();

            boolean found = false;
            RegionTaskWatcher changedFuture;
            int futuresRemaining = getOutstandingCount();
            SpliceLogUtils.trace(LOG,"Tasks remaining: %d",futuresRemaining);
            while(!found && futuresRemaining>0){
                changedFuture = changedTasks.take(); //block until one becomes available

                found = !completedTasks.contains(changedFuture) &&
                        !failedTasks.contains(changedFuture) &&
                        !cancelledTasks.contains(changedFuture);

                futuresRemaining = getOutstandingCount();

                if(found){
                    Status status = changedFuture.getStatus();
                    switch (status) {
                        case INVALID:
                            SpliceLogUtils.trace(LOG,"Task %s is invalid, resubmitting",changedFuture.getTaskNode());
                            invalidCount.incrementAndGet();
                            changedFuture.resubmit();
                            found=false;
                            break;
                        case FAILED:
                            try{
                                SpliceLogUtils.trace(LOG,"Task %s failed",changedFuture.getTaskNode());
                                stats.failedTasks.add(changedFuture.getTaskNode());
                                changedFuture.dealWithError();
                            }catch(ExecutionException ee){
                                failedTasks.add(changedFuture);
                                throw ee;
                            }
                            found=false;
                            break;
                        case COMPLETED:
                            try {
                                if (changedFuture.getTransactionId() != null) {
                                    final TransactorControl transactor = HTransactorFactory.getTransactorControl();
                                    TransactionId txnId = transactor.transactionIdFromString(changedFuture.getTransactionId());
                                    transactor.commit(txnId);
                                }
                            } catch (IOException e) {
                                failedTasks.add(changedFuture);
                                throw new ExecutionException(e);
                            }
                            SpliceLogUtils.trace(LOG,"Task %s completed successfully",changedFuture.getTaskNode());
                            TaskStats stats = changedFuture.getTaskStats();
                            if(stats!=null)
                                this.stats.taskStatsMap.put(changedFuture.getTaskNode(),stats);
                            completedTasks.add(changedFuture);
                            return;
                        case CANCELLED:
                            SpliceLogUtils.trace(LOG,"Task %s is cancelled",changedFuture.getTaskNode());
                            cancelledTasks.add(changedFuture);
                            throw new CancellationException();
                        default:
                            SpliceLogUtils.trace(LOG,"Task %s is in state %s",changedFuture.getTaskNode(),status);
                            found=false;
                    }
                }
            }
            if(getOutstandingCount()<=0){
                numRunning.decrementAndGet();
                if(failedTasks.size()>0){
                    totalFailed.incrementAndGet();
                } else if(cancelledTasks.size()>0){
                    totalCancelled.incrementAndGet();
                }else
                    totalCompleted.incrementAndGet();
            }
            SpliceLogUtils.trace(LOG,"completeNext finished");
        }

        @Override
        public void cancel() throws ExecutionException {
            try {
                ZkUtils.safeDelete(CoprocessorTaskScheduler.getJobPath()+"/"+job.getJobId(),-1);
                cancelled=true;
            } catch (KeeperException e) {
                throw new ExecutionException(e);
            } catch (InterruptedException e) {
                throw new ExecutionException(e);
            }
        }

        @Override
        public double getEstimatedCost() throws ExecutionException {
            double maxCost = 0d;
            for(TaskFuture future:tasksToWatch){
                if(maxCost < future.getEstimatedCost())
                    maxCost = future.getEstimatedCost();
            }
            return maxCost;
        }

        @Override
        public JobStats getJobStats() {
            return stats;
        }

        public void taskChanged(RegionTaskWatcher regionTaskWatcher) {
            changedTasks.offer(regionTaskWatcher);
        }

        private int getOutstandingCount(){
            return tasksToWatch.size()-completedTasks.size()-failedTasks.size()-cancelledTasks.size();
        }
    }
}
