package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.base.Function;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.job.*;
import com.splicemachine.tools.BalancedBlockingQueue;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.log4j.Logger;
import javax.annotation.Nullable;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 * Created on: 4/9/13
 */
public class SimpleThreadedTaskScheduler<T extends Task> implements TaskScheduler<T>,TaskSchedulerManagement {
    private static final Logger WORKER_LOG = Logger.getLogger(TaskCallable.class);
    private static final int DEFAULT_MAX_WORKERS = 10;
    private static final int DEFAULT_PRIORITY_LEVELS = 5;
    private static final int DEFAULT_INTERLEAVE_COUNT = 10;

    private static final Function<Runnable,Integer> priorityMapper = new Function<Runnable, Integer>() {
        @Override
        public Integer apply(@Nullable Runnable input) {
            if(input instanceof PriorityRunnable){
                return ((PriorityRunnable)input).getPriority();
            }
            return 1;
        }
    };

    private final ThreadPoolExecutor executor;

    private StatsListener statsListener = new StatsListener();

    private SimpleThreadedTaskScheduler(ThreadPoolExecutor executor) {
        this.executor = executor;
    }

    public static <T extends Task> SimpleThreadedTaskScheduler<T> create(Configuration configuration){
        int maxWorkers = configuration.getInt("splice.task.maxWorkers",DEFAULT_MAX_WORKERS);
        int numPriorityLevels = configuration.getInt("splice.task.priorityLevels", DEFAULT_PRIORITY_LEVELS);
        int tasksPerLevel = configuration.getInt("splice.task.priorityInterleave", DEFAULT_INTERLEAVE_COUNT);

        ThreadPoolExecutor executor = new TaskThreadPool(maxWorkers,maxWorkers,60,TimeUnit.SECONDS,
                new BalancedBlockingQueue<Runnable>(numPriorityLevels,tasksPerLevel,priorityMapper),
                new NamedThreadFactory());
        executor.allowCoreThreadTimeOut(true);

        return new SimpleThreadedTaskScheduler<T>(executor);
    }

    @Override
    public TaskFuture submit(T task) throws ExecutionException {
        ListeningFuture future = new ListeningFuture(task,statsListener.numPending.incrementAndGet());
        task.getTaskStatus().attachListener(statsListener);
        future.future = executor.submit(new TaskCallable<T>(task, statsListener));
        return future;
    }

/*********************************************************************************************************************/
    /*Statistics gathering*/
    @Override public int getNumPendingTasks() { return statsListener.numPending.get(); }
    @Override public int getCurrentWorkers() { return executor.getPoolSize(); }
    @Override public int getMaxWorkers() { return executor.getCorePoolSize(); }
    @Override public void setMaxWorkers(int newMaxWorkers) { executor.setCorePoolSize(newMaxWorkers); }
    @Override public long getTotalSubmittedTasks() { return executor.getTaskCount(); }
    @Override public long getTotalCompletedTasks() { return statsListener.completedCount.get(); }
    @Override public long getTotalFailedTasks() { return statsListener.failedCount.get(); }
    @Override public long getTotalCancelledTasks() { return statsListener.cancelledCount.get(); }
    @Override public long getTotalInvalidatedTasks() { return statsListener.invalidatedCount.get(); }
    @Override public int getNumRunningTasks() { return statsListener.numExecuting.get(); }

    @Override
    public int getHighestWorkerLoad() {
        if(executor.getPoolSize()>0)
            return executor.getQueue().size()/executor.getPoolSize();
        return 0; //no threads means the executor is idle, so we can't possibly have any tasks waiting
    }

    @Override
    public int getLowestWorkerLoad() {
        return getHighestWorkerLoad(); //in this model, all workers have the same load
    }

    private static class TaskCallable<T extends Task> implements Callable<Void> {
        private final T task;
        private final StatsListener listener;

        public TaskCallable(T task, StatsListener listener) {
            this.task = task;
            this.listener = listener;
        }

        @Override
        public Void call() throws Exception {
            try{
                switch (task.getTaskStatus().getStatus()) {
                    case INVALID:
                        SpliceLogUtils.trace(WORKER_LOG, "Task %s has been invalidated, cleaning up and skipping", task.getTaskId());
                        cleanUpTask(task);
                        return null;
                    case FAILED:
                        SpliceLogUtils.trace(WORKER_LOG,"Task %s has failed, but was not removed from the queue, removing now and skipping",task.getTaskId());
                        cleanUpTask(task);
                        return null;
                    case COMPLETED:
                        SpliceLogUtils.trace(WORKER_LOG, "Task %s has completed, but was not removed from the queue, removing now and skipping", task.getTaskId());
                        cleanUpTask(task);
                        return null;
                    case CANCELLED:
                        SpliceLogUtils.trace(WORKER_LOG,"task %s has been cancelled, not executing",task.getTaskId());
                        cleanUpTask(task);
                        return null;
                }
            }catch(ExecutionException ee){
                SpliceLogUtils.error(WORKER_LOG,
                        "task "+ task.getTaskId()+" had an unexpected error while checking status, unable to execute",ee.getCause());
                return null;
            }

            try{
                SpliceLogUtils.trace(WORKER_LOG,"executing task %s",task.getTaskId());
                try{
                    task.markStarted();
                }catch(CancellationException ce){
                    SpliceLogUtils.trace(WORKER_LOG,"task %s was cancelled",task.getTaskId());
                    return null;
                }
                task.execute();
                SpliceLogUtils.trace(WORKER_LOG,"task %s finished executing, marking completed",task.getTaskId());
                task.markCompleted();
            }catch(ExecutionException ee){
                Throwable t = ee.getCause();
                if(t instanceof NotServingRegionException){
                    /*
                     * We were accidentally assigned this task, but we aren't responsible for it, so we need
                     * to invalidate it and send it back to the client to re-submit
                     */
                    SpliceLogUtils.trace(WORKER_LOG,"task %s was assigned to the incorrect region, invalidating:%s",task.getTaskId(),t.getMessage());
                    task.markInvalid();
                }else{
                    SpliceLogUtils.error(WORKER_LOG,"task "+ task.getTaskId()+" had an unexpected error",ee.getCause());
                    try{
                        task.markFailed(ee.getCause());
                    }catch(ExecutionException failEx){
                        SpliceLogUtils.error(WORKER_LOG,"Unable to indicate task failure",failEx.getCause());
                    }
                }
            }catch(Throwable t){
                SpliceLogUtils.error(WORKER_LOG, "task " + task.getTaskId() + " had an unexpected error while setting state", t);
                try{
                    task.markFailed(t);
                }catch(ExecutionException failEx){
                    SpliceLogUtils.error(WORKER_LOG,"Unable to indicate task failure",failEx.getCause());
                }
            }
            return null;
        }

        private void cleanUpTask(T task)  throws ExecutionException{
            //if we clean up the task, then we never got a chance to decrement the pending count
            listener.numPending.decrementAndGet();
            task.cleanup();
        }
    }

    private static class NamedThreadFactory implements ThreadFactory {
        private final AtomicLong threadCount = new AtomicLong(0l);
        private final Thread.UncaughtExceptionHandler eh = new ExceptionLogger();
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("taskWorker-"+threadCount.incrementAndGet());
            t.setDaemon(true);
            t.setUncaughtExceptionHandler(eh);
            return t;
        }
    }

    private static final class ExceptionLogger implements Thread.UncaughtExceptionHandler{
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            WORKER_LOG.error("Worker Thread t failed with unexpected exception: ",e);
        }
    }

    private class ListeningFuture implements TaskFuture,TaskStatus.StatusListener {
        private Future<Void> future;
        private final Task task;
        private final double cost;

        public ListeningFuture(Task task, double estCost) {
            this.task = task;
            this.cost = estCost;
        }

        @Override
        public Status getStatus() throws ExecutionException {
            return task.getTaskStatus().getStatus();
        }

        @Override
        public void complete() throws ExecutionException, CancellationException, InterruptedException {
            future.get();
        }

        @Override
        public double getEstimatedCost() {
            return cost;
        }

        //@Override
        public void cancel() throws ExecutionException {
            task.markCancelled();
            future.cancel(true);
        }

        @Override
        public byte[] getTaskId() {
            return task.getTaskId();
        }

        @Override
        public TaskStats getTaskStats() {
            return task.getTaskStatus().getStats();
        }

        @Override
        public void statusChanged(Status oldStatus, Status newStatus, TaskStatus taskStatus) {
            switch (oldStatus) {
                case FAILED:
                case COMPLETED:
                case CANCELLED:
                    return;
            }

            switch (newStatus) {
                case CANCELLED:
                    future.cancel(true);
            }
        }
    }

    private static class StatsListener implements TaskStatus.StatusListener{
        /*Statistics for management and monitoring*/
        private final AtomicLong completedCount = new AtomicLong(0l);
        private final AtomicLong failedCount = new AtomicLong(0l);
        private final AtomicLong cancelledCount = new AtomicLong(0l);
        private final AtomicLong invalidatedCount = new AtomicLong(0l);

        private final AtomicInteger numPending = new AtomicInteger(0);
        private final AtomicInteger numExecuting = new AtomicInteger(0);

        @Override
        public void statusChanged(Status oldStatus, Status newStatus, TaskStatus taskStatus) {
            switch (oldStatus) {
                case PENDING:
                    numPending.decrementAndGet();
                    break;
                case EXECUTING:
                    numExecuting.decrementAndGet();
                    break;
            }
            switch (newStatus) {
                case FAILED:
                    failedCount.incrementAndGet();
                    taskStatus.detachListener(this);
                    return;
                case COMPLETED:
                    completedCount.incrementAndGet();
                    taskStatus.detachListener(this);
                    return;
                case CANCELLED:
                    cancelledCount.incrementAndGet();
                    taskStatus.detachListener(this);
                case INVALID:
                    invalidatedCount.incrementAndGet();
                    taskStatus.detachListener(this);
                    return;
                case EXECUTING:
                    numExecuting.incrementAndGet();
            }
        }
    }

    private static class TaskThreadPool extends ThreadPoolExecutor{

        public TaskThreadPool(int corePoolSize, int maximumPoolSize,
                              long keepAliveTime, TimeUnit unit,
                              BlockingQueue<Runnable> workQueue, ThreadFactory threadFactory) {
            super(corePoolSize, maximumPoolSize, keepAliveTime, unit, workQueue, threadFactory);
        }

        @Override
        protected <T> RunnableFuture<T> newTaskFor(Callable<T> callable) {
            if(callable instanceof TaskCallable)
                return new PriorityTaskRunnableFuture<T>(callable,((TaskCallable)callable).task.getPriority());
            //when in doubt, just default to the original
            return super.newTaskFor(callable);
        }
    }

    private static class PriorityTaskRunnableFuture<T> extends FutureTask<T> implements PriorityRunnable<T>{

        private final int priority;

        public PriorityTaskRunnableFuture(Callable<T> callable,int priority) {
            super(callable);
            this.priority = priority;
        }

        @Override
        public int getPriority() {
            return priority;
        }
    }
}
