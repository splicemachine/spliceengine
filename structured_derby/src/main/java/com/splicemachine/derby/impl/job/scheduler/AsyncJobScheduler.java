package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.base.Preconditions;
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
import com.splicemachine.hbase.writer.WriteUtils;
import com.splicemachine.job.*;
import com.splicemachine.si.api.HTransactorFactory;
import com.splicemachine.si.api.TransactionStatus;
import com.splicemachine.si.api.TransactorControl;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.ByteDataInput;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import com.splicemachine.utils.ZkUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.RetriesExhaustedException;
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
    private static final int DEFAULT_MAX_RESUBMISSIONS = 20;
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
    	if (LOG.isTraceEnabled())
    		SpliceLogUtils.trace(LOG,"submit job with watcher %s",job.getJobId());

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
                        	if (LOG.isTraceEnabled())
                        		SpliceLogUtils.trace(LOG,"submission callback issues %s",result);
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
            if(taskStatus.shouldRetry()) {
                resubmit();
            } else {
                throw new ExecutionException(Exceptions.getWrapperException(taskStatus.getErrorMessage(),taskStatus.getErrorCode()));
            }
        }

        private void resubmit() throws ExecutionException{
        	if (LOG.isDebugEnabled())
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
            rollbackChildTransaction();

            byte[] endRow;
            if(nextTask!=null){
                endRow = new byte[nextTask.startRow.length];
                System.arraycopy(nextTask.startRow,0,endRow,0,endRow.length);

                BytesUtil.unsignedDecrement(endRow,endRow.length-1);
            }else
                endRow = HConstants.EMPTY_END_ROW;

            try{
                Pair<RegionTask,Pair<byte[],byte[]>> newTaskData = job.resubmitTask(task, startRow, endRow);
            	if (LOG.isTraceEnabled())
            		SpliceLogUtils.trace(LOG,"executing submit on resubmitted job %s",job);
                //resubmit the task
                submit(watcher,job, newTaskData.getFirst(), newTaskData.getSecond(), table,tryNum+1);
            }catch(IOException ioe){
                throw new ExecutionException(ioe);
            }
        }

        private void rollbackChildTransaction() throws ExecutionException {
            //rollback child transaction
            if(taskStatus.getTransactionId()!=null){
                try {
                	if (LOG.isTraceEnabled())
                		SpliceLogUtils.trace(LOG, "rolling back transaction %s during resubmission", taskStatus.getTransactionId());
                    final TransactorControl transactor = HTransactorFactory.getTransactorControl();
                    TransactionId txnId = transactor.transactionIdFromString(taskStatus.getTransactionId());
                    transactor.rollback(txnId);
                } catch (IOException e) {
                    Exception error = new DoNotRetryIOException("Unable to roll back child transaction",e);
                    taskStatus.setError(error);
                    taskStatus.setStatus(Status.FAILED);
                    throw new ExecutionException(error);
                }
            }else{
                //no child transaction has been assigned, so we don't have anything to roll back,
                //but we want to issue a warning just in case, to make sure this isn't a programmer error
                LOG.warn("No Child transaction has been assigned to task "+ getTaskNode());
            }
        }

        boolean commit(int tryCount) throws ExecutionException{
            if(taskStatus.getTransactionId()==null){
                //there is nothing to commit, which is weird--most of the time there should be.
                //HOWEVER, sometimes there are tasks which do not operate transactionally
                //those tasks should just return here. Just to make sure that nothing
                //accidentally slips through though, we assert that our task isn't transactional
                Preconditions.checkArgument(!task.isTransactional(),"Transactional task does not have an associated transaction id");
                return true;
            }

             /*
              * Child transactions can't be committed by the task, because there is an inherent failure
              * condition. If the Child transaction successfully commits, but the task is unable to
              * report it's status to the JobScheduler, The Scheduler will attempt to resubmit that task,
              * which would then see data from itself (resulting in extraneous PrimaryKey violations and
              * other such errors).
              */
            final TransactorControl transactor = HTransactorFactory.getTransactorControl();
            TransactionId txnId = transactor.transactionIdFromString(taskStatus.getTransactionId());
            if(tryCount<0){
                /*
                 * We couldn't commit this, but we could retry the entire task and maybe that one we'll be able to commit
                 */
                taskStatus.setStatus(Status.FAILED);
                taskStatus.setError(new RetriesExhaustedException("Unable to commit transaction " + txnId+" after "+ maxResubmissionAttempts+" attempts"));
                dealWithError();
                return false;
            }
            try{
                transactor.commit(txnId);
                return true;
            }catch(IOException ioe){
                    /*
                     * We failed to commit for some reason or another. This is probably bad, but we want to recover
                     * if at all possible.
                     *
                     * Unfortunately, just because we got an error doesn't mean that we failed to commit. Writes
                     * to the transaction table are atomic, but our failures are not (we could have failed before
                     * or after the table accepted the write). Thus, we must check back with the transaction
                     * table to determine the current state of our Transaction. If it's still active, then we
                     * can retry the commit. If it's in the FAILED state, then this child transaction failed. We have
                     * to treat the entire task as failed and resubmit it.
                     *
                     * If we try reading the transaction state for a few tries and are unable to do so, then we must
                     * assume the entire parent transaction (and probably this machine) is completely screwed, and fail
                     * the job.
                     */
                TransactionStatus txnStatus;
                try {
                    txnStatus = getTransactionStatus(txnId,transactor,maxResubmissionAttempts);
                } catch (IOException e) {
                    //we can't even read the txn information, we are really screwed, so make job fail
                    throw new ExecutionException(new RetriesExhaustedException("Unable to obtain transaction status, assuming transaction dead",e));
                }
                switch (txnStatus) {
                    case COMMITTED:
                        //we successfully committed
                        return true;
                    case ACTIVE:
                        //we can retry commit
                        return commit(tryCount-1);
                    case COMMITTING:
                        //should never see this
                        throw new ExecutionException(new IllegalStateException("Transaction "+ txnId+" is in state COMMITTING, which should never be seen here"));
                    default:
                            /*
                             * We cannot retry this transaction. However, we CAN retry the entire child task
                             */
                        this.taskStatus.setError(ioe);
                        this.taskStatus.setStatus(Status.FAILED);
                        dealWithError();
                        return false;
                }
            }
        }

        private TransactionStatus getTransactionStatus(TransactionId txnId,TransactorControl transactor, int tryCount) throws IOException {
            if(tryCount<=0)
                throw new RetriesExhaustedException("Unable to read transaction information for transaction "+ txnId);
            try{
                return transactor.getTransactionStatus(txnId);
            }catch(IOException ioe){
               //TODO -sf- make sure these are the correct retry semantics
               if(ioe instanceof DoNotRetryIOException){
                   throw ioe;
               }
                try {
                    Thread.sleep(WriteUtils.getWaitTime(maxResubmissionAttempts-tryCount+1,1000));
                } catch (InterruptedException e) {
                    //interruption just means to keep going
                    LOG.info("Interrupted while waiting to read from transaction table");
                }

                return getTransactionStatus(txnId, transactor, tryCount-1);
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
        private List<byte[]> failedTasks = Collections.synchronizedList(Lists.<byte[]>newArrayListWithExpectedSize(0));

        private final AtomicInteger tasks = new AtomicInteger(0);
        private final AtomicInteger submittedTasks = new AtomicInteger();

        private final long start = System.nanoTime();

        private JobStatsAccumulator(Watcher watcher) {
            this.watcher = watcher;
        }

        @Override public int getNumTasks() { return tasks.get(); }
        @Override public long getTotalTime() { return System.nanoTime()-start; }
        @Override public int getNumSubmittedTasks() { return submittedTasks.get(); }
        @Override public int getNumCompletedTasks() { return watcher.completedTasks.size(); }
        @Override public int getNumFailedTasks() { return failedTasks.size(); }
        @Override public int getNumInvalidatedTasks() { return watcher.invalidCount.get(); }
        @Override public int getNumCancelledTasks() { return watcher.cancelledTasks.size(); }
        @Override public Map<String, TaskStats> getTaskStats() { return taskStatsMap; }
        @Override public String getJobName() { return watcher.job.getJobId(); }
        @Override public List<byte[]> getFailedTasks() { return failedTasks; }
    }

    private class Watcher implements JobFuture {
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
        public int getRemainingTasks() {
            return getOutstandingCount();
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
        public int getNumTasks() {
            return tasksToWatch.size();
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
                                stats.failedTasks.add(changedFuture.getTaskId());
                                changedFuture.dealWithError();
                            }catch(ExecutionException ee){
                                failedTasks.add(changedFuture);
                                throw ee;
                            }
                            found=false;
                            break;
                        case COMPLETED:
                            SpliceLogUtils.trace(LOG,"Task %s completed successfully",changedFuture.getTaskNode());
                            try{
                                /*
                                 * Attempt to commit. Three things are possible:
                                 *
                                 * 1. the commit succeeds and returns true
                                 * 2. The commit fails in a retryable fashion, and returns false
                                 * 3. The commit fails in a non-retryable fashion (throws exception)
                                 */
                                if(changedFuture.commit(maxResubmissionAttempts)){
                                   //we committed successfully, whoo!
                                    TaskStats stats = changedFuture.getTaskStats();
                                    if(stats!=null)
                                        this.stats.taskStatsMap.put(changedFuture.getTaskNode(),stats);
                                    completedTasks.add(changedFuture);
                                    return;
                                }else{
                                    //we had to resubmit
                                    SpliceLogUtils.trace(LOG,"Task %s was unable to commit, but has been resubmitted",changedFuture.getTaskNode());
                                }
                            }catch(Throwable t){
                                //we cannot retry, so add this to failed tasks
                                changedFuture.taskStatus.setError(t);
                                changedFuture.taskStatus.setStatus(Status.FAILED);
                                failedTasks.add(changedFuture);
                                throw new ExecutionException(t);
                            }
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
