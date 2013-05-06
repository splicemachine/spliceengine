package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorTaskScheduler;
import com.splicemachine.derby.impl.job.coprocessor.TaskFutureContext;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.ByteDataInput;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceZooKeeperManager;
import com.splicemachine.derby.utils.ZkUtils;
import com.splicemachine.job.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public abstract class ZkBackedJobScheduler<J extends Job> implements JobScheduler<J>,JobSchedulerManagement{
    protected final SpliceZooKeeperManager zkManager;

    private final AtomicLong totalSubmitted = new AtomicLong(0l);
    private final AtomicLong totalCompleted = new AtomicLong(0l);
    private final AtomicLong totalFailed = new AtomicLong(0l);
    private final AtomicLong totalCancelled = new AtomicLong(0l);
    private final AtomicInteger numRunning = new AtomicInteger(0);

    public ZkBackedJobScheduler(SpliceZooKeeperManager zooKeeper) {
        this.zkManager = zooKeeper;
    }

    @Override
    public JobFuture submit(final J job) throws ExecutionException {
        totalSubmitted.incrementAndGet();
        try {
            String jobPath = createJobNode(job);
            WatchingFuture future = new WatchingFuture(jobPath,submitTasks(job));
            future.attachWatchers();
            numRunning.incrementAndGet();
            return future;
        } catch (Throwable throwable) {
            Throwable root = Throwables.getRootCause(throwable);
            throw new ExecutionException(root);
        }
    }

    private String createJobNode(J job) throws KeeperException, InterruptedException {
        String jobId = job.getJobId();
        jobId = jobId.replaceAll("/","_");

        String path = CoprocessorTaskScheduler.getJobPath()+"/"+jobId;
        ZkUtils.recursiveSafeCreate(path,
                new byte[]{},ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.EPHEMERAL);

        return path;
    }

    @Override
    public void cleanupJob(JobFuture future) throws ExecutionException {
        //make sure that we CAN clean up this job
        Preconditions.checkArgument(
                WatchingFuture.class.isAssignableFrom(future.getClass()),
                "unknown JobFuture type: "+ future.getClass());

        WatchingFuture futureToClean = (WatchingFuture)future;
        futureToClean.cleanup();
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

    protected abstract Set<? extends WatchingTask> submitTasks(J job) throws ExecutionException;

    protected class WatchingFuture implements JobFuture{
        private final Logger logger = Logger.getLogger(WatchingFuture.class);
        private final Collection<? extends WatchingTask> taskFutures;
        private final BlockingQueue<TaskFuture> changedFutures;
        private final Set<TaskFuture> completedFutures;
        private final Set<TaskFuture> failedFutures;
        private final Set<TaskFuture> cancelledFutures;
        private final AtomicInteger invalidatedCount = new AtomicInteger(0);
        private final ZkJobStats stats;
        private final String jobPath;

        private volatile Status currentStatus = Status.PENDING;

        private WatchingFuture(String jobPath,Collection<? extends WatchingTask> taskFutures) {
            this.taskFutures = taskFutures;
            this.jobPath = jobPath;
            this.changedFutures = new LinkedBlockingQueue<TaskFuture>();
            this.completedFutures = Collections.newSetFromMap(new ConcurrentHashMap<TaskFuture, Boolean>());
            this.failedFutures = Collections.newSetFromMap(new ConcurrentHashMap<TaskFuture, Boolean>());
            this.cancelledFutures = Collections.newSetFromMap(new ConcurrentHashMap<TaskFuture, Boolean>());
            this.stats = new ZkJobStats(jobPath,this);
        }

        private void attachWatchers() throws ExecutionException {
            for(WatchingTask taskFuture:taskFutures){
                taskFuture.attachJobFuture(this);
                /*
                 * We know, because we know the type of the TaskFuture, that calling getStatus()
                 * will actually attach a watcher for us, so we don't have to worry about reattaching.
                 */
                Status status = taskFuture.getStatus();
                SpliceLogUtils.trace(logger,"Task %s is %s",taskFuture.getTaskId(),status);
                switch (status) {
                    case INVALID:
                        invalidatedCount.incrementAndGet();
                        taskFuture.manageInvalidated();
                        break;
                    case FAILED:
                        failedFutures.add(taskFuture);
                        break;
                    case COMPLETED:
                        completedFutures.add(taskFuture);
                        break;
                    case EXECUTING:
                        currentStatus = Status.EXECUTING;
                        break;
                    case CANCELLED:
                        cancelledFutures.add(taskFuture);
                        break;
                }
            }

            //attach job watcher to watch for job cancellation from JMX
            try {
                zkManager.execute(new SpliceZooKeeperManager.Command<Void>() {
                    @Override
                    public Void execute(RecoverableZooKeeper zooKeeper) throws InterruptedException, KeeperException {
                        zooKeeper.exists(jobPath, new Watcher() {
                            @Override
                            public void process(WatchedEvent event) {
                                try {
                                    cancel();
                                } catch (ExecutionException e) {
                                    throw new RuntimeException(Exceptions.parseException(e.getCause()));
                                }
                            }
                        });
                        return null;  //To change body of implemented methods use File | Settings | File Templates.
                    }
                });
            } catch (KeeperException e) {
                throw new ExecutionException(e);
            } catch (InterruptedException e) {
                throw new ExecutionException(e);
            }
        }

        @Override
        public Status getStatus() throws ExecutionException {
            if(failedFutures.size()>0) return Status.FAILED;
            else if(cancelledFutures.size()>0) return Status.CANCELLED;
            else if (completedFutures.size()>=taskFutures.size()) return Status.COMPLETED;
            else return currentStatus;
        }

        @Override
        public void completeAll() throws ExecutionException, InterruptedException,CancellationException {
            while(getOutstandingCount()>0){
                completeNext();
            }
            numRunning.decrementAndGet();
        }

        @Override
        public void completeNext() throws ExecutionException, InterruptedException,CancellationException {
            if(failedFutures.size()>0){
                for(TaskFuture future:failedFutures) future.complete(); //throw the task error
            }else if(cancelledFutures.size()>0)
                throw new CancellationException();

            //wait for the next Future to be changed
            boolean found=false;
            TaskFuture changedFuture;
            int futuresRemaining = getOutstandingCount();
            while(!found&&futuresRemaining>0){
                changedFuture = changedFutures.take();
                SpliceLogUtils.trace(logger,"Received a task with a changed status, updating state");

                found = !completedFutures.contains(changedFuture) &&
                        !failedFutures.contains(changedFuture) &&
                        !cancelledFutures.contains(changedFuture);
                futuresRemaining = getOutstandingCount();

                if(found){
                    Status status = changedFuture.getStatus();
                    switch (status) {
                        case FAILED:
                            SpliceLogUtils.trace(logger, "Task "+ changedFuture.getTaskId()+" failed");
                            failedFutures.add(changedFuture);
                            changedFuture.complete(); //will throw an ExecutionException immediately
                            break;
                        case COMPLETED:
                            SpliceLogUtils.trace(logger, "Task "+ changedFuture.getTaskId()+" completed");
                            TaskStats stats = changedFuture.getTaskStats();
                            if(stats!=null)
                                this.stats.taskStatsMap.put(changedFuture.getTaskId(),stats);
                            completedFutures.add(changedFuture); //found the next completed task
                            return;
                        case CANCELLED:
                            SpliceLogUtils.trace(logger, "Task "+ changedFuture.getTaskId()+" was cancelled");
                            cancelledFutures.add(changedFuture);
                            throw new CancellationException();
                        case INVALID:
                            SpliceLogUtils.trace(logger,"Task "+ changedFuture.getTaskId()+" was invalidated, managing");
                            ((WatchingTask)changedFuture).manageInvalidated();
                        default:
                            SpliceLogUtils.trace(logger,"Unable to determine state of task "+ changedFuture.getTaskId());
                            found=false; //circle around because we aren't finished yet
                    }
                }
            }
            if(getOutstandingCount()<=0){
                numRunning.decrementAndGet();
                if(failedFutures.size()>0){
                    totalFailed.incrementAndGet();
                } else if(cancelledFutures.size()>0){
                    totalCancelled.incrementAndGet();
                }else
                    totalCompleted.incrementAndGet();
            }
        }

        private int getOutstandingCount() {
            return taskFutures.size()-completedFutures.size()
                -failedFutures.size()
                -cancelledFutures.size();
        }

        @Override
        public void cancel() throws ExecutionException {
            for(TaskFuture future:taskFutures){
                future.cancel();
                if(future.getStatus()==Status.CANCELLED)
                    changedFutures.offer(future);
            }
        }

        @Override
        public double getEstimatedCost() throws ExecutionException {
            double maxCost = 0d;
            for(TaskFuture future:taskFutures){
                if(maxCost< future.getEstimatedCost())
                    maxCost = future.getEstimatedCost();
            }
            return maxCost;
        }

        @Override
        public JobStats getJobStats() {
            return stats;
        }

        public void cleanup() throws ExecutionException{
            //delete the job node
            try {
                zkManager.getRecoverableZooKeeper().delete(jobPath, -1);
            } catch (InterruptedException e) {
                throw new ExecutionException(e);
            } catch (KeeperException e) {
                if(e.code()!= KeeperException.Code.NONODE)
                    throw new ExecutionException(e);
            } catch (ZooKeeperConnectionException e) {
                throw new ExecutionException(e);
            }

            for(WatchingTask task:taskFutures){
               task.cleanup();
            }
        }
    }

    protected class ZkJobStats implements JobStats{
        private final WatchingFuture future;
        private final Map<String,TaskStats> taskStatsMap = new ConcurrentHashMap<String, TaskStats>();
        private final long start = System.nanoTime();
        private final String jobName;

        public ZkJobStats(String jobName,WatchingFuture future) {
            this.future = future;
            this.jobName = jobName;
        }

        @Override
        public int getNumTasks() {
            return future.taskFutures.size();
        }

        @Override
        public long getTotalTime() {
            return System.nanoTime()-start;
        }

        @Override
        public int getNumSubmittedTasks()  {
            return future.taskFutures.size()-getNumInvalidatedTasks();
        }

        @Override
        public int getNumCompletedTasks(){
            return future.completedFutures.size();
        }

        @Override
        public int getNumFailedTasks() {
            return future.failedFutures.size();
        }

        @Override
        public int getNumInvalidatedTasks() {
            return future.invalidatedCount.get();
        }

        @Override
        public int getNumCancelledTasks() {
            return future.cancelledFutures.size();
        }

        @Override
        public Map<String, TaskStats> getTaskStats() {
            return taskStatsMap;
        }

        @Override
        public String getJobName() {
            return jobName.substring(jobName.lastIndexOf('/')+1);
        }
    }

    protected abstract class WatchingTask implements TaskFuture,Watcher {
        protected final Logger logger;
        protected final TaskFutureContext context;
        private final SpliceZooKeeperManager zkManager;
        private WatchingFuture jobFuture;

        protected volatile TaskStatus status = new TaskStatus(Status.PENDING,null);
        private volatile boolean refresh = true;

        public WatchingTask(TaskFutureContext result, SpliceZooKeeperManager zkManager) {
            this.zkManager = zkManager;
            logger = Logger.getLogger(this.getClass());
            this.context = result;
        }

        void attachJobFuture(WatchingFuture jobFuture){
            this.jobFuture = jobFuture;
        }

        @Override
        public Status getStatus() throws ExecutionException {
            //these three status are permanent--once they have been entered, they cannot be escaped
            //so no reason to refresh even if fired (which it should never be)
            switch (status.getStatus()) {
                case INVALID:
                case COMPLETED:
                case FAILED:
                case CANCELLED:
                   return status.getStatus();
            }
            if(!refresh) return status.getStatus();
            else{
                try {
                    byte[] data = zkManager.executeUnlessExpired(new SpliceZooKeeperManager.Command<byte[]>() {
                        @Override
                        public byte[] execute(RecoverableZooKeeper zooKeeper) throws InterruptedException, KeeperException {
                            try{
                                return zooKeeper.getData(context.getTaskNode()+"/status",WatchingTask.this,new Stat());
                            }catch(KeeperException ke){
                                if(ke.code()== KeeperException.Code.NONODE){
                                    /*
                                     * The status node can only be missing if
                                     *
                                     * 1. I delete it
                                     * 2. it was removed from ZooKeeper because a session expired on
                                     * the task executor.
                                     *
                                     * since I clearly didn't delete it, it must have been a session expired. Assume
                                     * that the task failed and blow up.
                                     *
                                     */
                                    return null;
                                }
                                throw ke;
                            }
                        }
                    });
                    if(data==null){
                        //TODO -sf- this should be treated as retryable, in which case we need to
                        // rollback the child transaction and restart
                        status = TaskStatus.failed("task executor failed to update status for task "+getTaskId()+", failing job");
                        refresh=false;
                        return status.getStatus();
                    }else{
                        ByteDataInput bdi = new ByteDataInput(data);
                        try {
                            status = (TaskStatus)bdi.readObject();
                        } catch (ClassNotFoundException e) {
                            throw new ExecutionException(e);
                        } catch (IOException e) {
                            throw new ExecutionException(e);
                        }
                        refresh=false;
                        return status.getStatus();
                    }

                } catch (InterruptedException e) {
                    throw new ExecutionException(e);
                } catch (KeeperException e) {
                    if(e.code()== KeeperException.Code.SESSIONEXPIRED){
                        /*
                         * We've been expired. That's bad news, it means our job was cancelled out from
                         * under us, and so all tasks should be treated as cancelled, and a failure should be returned
                         */
                        status = TaskStatus.failed("Controller Session expired");
                        refresh=false;
                        return status.getStatus();
                    }else
                        throw new ExecutionException(e);
                }
            }
        }

        @Override
        public void complete() throws ExecutionException, CancellationException, InterruptedException {
            while(true){
                Status runningStatus = getStatus();
                switch (runningStatus) {
                    case INVALID:
                        jobFuture.invalidatedCount.incrementAndGet();
                        manageInvalidated();
                    case FAILED:
                        throw new ExecutionException(status.getError());
                    case COMPLETED:
                        return;
                    case CANCELLED:
                        throw new CancellationException();
                }
                //put thread to sleep until status has changed
                synchronized (context){
                    context.wait();
                }
            }
        }

        protected abstract void manageInvalidated() throws ExecutionException;

        @Override
        public double getEstimatedCost() {
            return context.getEstimatedCost();
        }

        @Override
        public void process(WatchedEvent event) {
            SpliceLogUtils.trace(logger, "received notification that task "+ event.getPath()+" has changed status");
            refresh=true;
            jobFuture.changedFutures.offer(this);
            synchronized (context){
                context.notifyAll();
            }
        }

        @Override
        public void cancel() throws ExecutionException {
            //nothing to do if it's in one of these states
            switch (status.getStatus()) {
                case FAILED:
                case COMPLETED:
                case CANCELLED:
                case INVALID:
                    return;
            }
            status = TaskStatus.cancelled();
        }

        @Override
        public TaskStats getTaskStats() {
            return status.getStats();
        }

        @Override
        public String getTaskId() {
            return context.getTaskNode();
        }

        public void cleanup() throws ExecutionException {
            RecoverableZooKeeper zooKeeper;
            try {
                zooKeeper = zkManager.getRecoverableZooKeeper();
            } catch (ZooKeeperConnectionException e) {
                throw new ExecutionException(e);
            }

            try{
                zooKeeper.delete(context.getTaskNode() + "/status", -1);
            }catch(KeeperException e){
                //ignore it if it's already deleted
               if(e.code()!= KeeperException.Code.NONODE)
                  throw new ExecutionException(e);
            } catch (InterruptedException e) {
                throw new ExecutionException(e);
            }
            try{
                zooKeeper.delete(context.getTaskNode(),-1);
            } catch (InterruptedException e) {
                throw new ExecutionException(e);
            } catch (KeeperException e) {
                //ignore the error if the node isn't present
                if(e.code()!= KeeperException.Code.NONODE)
                    throw new ExecutionException(e);
            }
        }
    }
}
