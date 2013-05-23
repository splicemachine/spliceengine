package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.collect.Maps;
import com.splicemachine.constants.bytes.BytesUtil;
import com.splicemachine.derby.impl.job.coprocessor.*;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.ByteDataInput;
import com.splicemachine.job.*;
import com.splicemachine.si.api.TransactionId;
import com.splicemachine.si.api.com.splicemachine.si.api.hbase.HTransactor;
import com.splicemachine.si.api.com.splicemachine.si.api.hbase.HTransactorFactory;
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
import java.util.concurrent.*;
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

        TransactionId parentTxn = job.getParentTransaction();
        boolean readOnly = job.isReadOnly();

        Watcher watcher = new Watcher(job);
        for(final RegionTask task:tasks.keySet()){
            submit(watcher,task, tasks.get(task), table,0);
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
                            RegionTaskWatcher taskWatcher = new RegionTaskWatcher(watcher, row, task, result, table,tryNum);
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
        private final HTableInterface table;
        private final Watcher watcher;
        private final TaskFutureContext taskFutureContext;
        private TaskStatus taskStatus = new TaskStatus(Status.PENDING,null);
        private volatile boolean refresh = true;
        private final int tryNum;

        private RegionTaskWatcher(Watcher watcher,
                                  byte[] startRow,
                                  RegionTask task,
                                  TaskFutureContext taskFutureContext,
                                  HTableInterface table,
                                  int tryNum) {
            this.watcher = watcher;
            this.startRow = startRow;
            this.task = task;
            this.table = table;
            this.taskFutureContext = taskFutureContext;
            this.tryNum = tryNum;
        }

        @Override
        public int compareTo(RegionTaskWatcher o) {
            if(o==null) return 1;
            return Bytes.compareTo(startRow, o.startRow);
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
                    refresh=false;
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
            Throwable error = taskStatus.getError();
            //TODO -sf- is this an adequate guard?
            if(error instanceof DoNotRetryIOException
                    || error instanceof StandardException ){
                throw new ExecutionException(error);
            }else if(error instanceof RetriesExhaustedWithDetailsException){
                List<Throwable> errors = ((RetriesExhaustedWithDetailsException)error).getCauses();
                for(Throwable e:errors){
                    if(e instanceof DoNotRetryIOException||e instanceof StandardException)
                        throw new ExecutionException(e);
                }
                //retryable error
                resubmit();
            }else{
                //retryable error
                resubmit();
            }
        }

        private void resubmit() throws ExecutionException{
            SpliceLogUtils.debug(LOG,"resubmitting task %s",task.getTaskId());
            //only submit so many time
            if(tryNum>=maxResubmissionAttempts){
                ExecutionException ee = new ExecutionException(
                        new RetriesExhaustedException("Unable to complete task "+ taskFutureContext.getTaskNode()+", it was invalidated more than "+ maxResubmissionAttempts+" times"));
                taskStatus.setError(ee.getCause());
                taskStatus.setStatus(Status.FAILED);
                throw ee;
            }

            RegionTaskWatcher nextTask = watcher.tasksToWatch.higher(this);

            watcher.tasksToWatch.remove(this);
            //rollback child transaction
            if(taskStatus.getTransactionId()!=null){
                try {
                    HTransactor transactor = HTransactorFactory.getTransactor();
                    TransactionId txnId = transactor.transactionIdFromString(taskStatus.getTransactionId());
                    HTransactorFactory.getTransactor().rollback(txnId);
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

            //resubmit the task
            submit(watcher, task, Pair.newPair(startRow, endRow), table,tryNum+1);
        }

        @Override
        public double getEstimatedCost() {
            return taskFutureContext.getEstimatedCost();
        }

        @Override
        public String getTaskId() {
            return taskFutureContext.getTaskNode();
        }

        @Override
        public TaskStats getTaskStats() {
            return taskStatus.getStats();
        }

    }

    private static class JobStatsAccumulator implements JobStats{
        private Map<String,TaskStats> taskStatsMap = Maps.newConcurrentMap();
        private Watcher watcher;

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
            return watcher.failedTasks.size();
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

                        for(TaskFuture task:tasksToWatch){
                            try{
                                zooKeeper.delete(task.getTaskId(),-1);
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
                            SpliceLogUtils.trace(LOG,"Task %s is invalid, resubmitting",changedFuture.getTaskId());
                            invalidCount.incrementAndGet();
                            changedFuture.resubmit();
                            break;
                        case FAILED:
                            try{
                                SpliceLogUtils.trace(LOG,"Task %s failed",changedFuture.getTaskId());
                                changedFuture.dealWithError();
                            }catch(ExecutionException ee){
                                failedTasks.add(changedFuture);
                                throw ee;
                            }
                            break;
                        case COMPLETED:
                            SpliceLogUtils.trace(LOG,"Task %s completed successfully",changedFuture.getTaskId());
                            TaskStats stats = changedFuture.getTaskStats();
                            if(stats!=null)
                                this.stats.taskStatsMap.put(changedFuture.getTaskId(),stats);
                            completedTasks.add(changedFuture);
                            return;
                        case CANCELLED:
                            SpliceLogUtils.trace(LOG,"Task %s is cancelled",changedFuture.getTaskId());
                            cancelledTasks.add(changedFuture);
                            throw new CancellationException();
                        default:
                            SpliceLogUtils.trace(LOG,"Task %s is in state %s",changedFuture.getTaskId(),status);
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
