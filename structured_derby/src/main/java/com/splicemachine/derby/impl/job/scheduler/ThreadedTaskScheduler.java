package com.splicemachine.derby.impl.job.scheduler;

import com.splicemachine.job.TaskStatus;
import com.splicemachine.job.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Threaded implementation of a TaskScheduler.
 *
 * This implementation makes use of work-stealing to distribute work across multiple worker threads
 * in a fair and efficient manner.
 *
 * In essence, there are {@code n} worker threads which are constantly pulling work from their own internal queue.
 * However, if its queue is empty, then a worker thread will attempt to pull work off the back of another worker's
 * internal queue. If no work is available on any worker's queue, then the worker will fall back to waiting for
 * work to appear on its own queue again, to avoid excessive CPU time spinning over queues.
 *
 * This technique works effectively because the Scheduler will attempt to evenly distribute work across all
 * worker threads. That is, all worker threads will be given a task before any worker thread is given a second task.
 * This makes a thread confident that work will eventually arrive to be processed before any one worker gets overloaded
 * with tasks.
 *
 *
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class  ThreadedTaskScheduler<T extends Task> implements TaskScheduler<T>,TaskSchedulerManagement {
    private static final Logger LOG = Logger.getLogger(ThreadedTaskScheduler.class);
    private final int numWorkers;
    private AtomicReference< Worker> nextWorker = new AtomicReference<Worker>();
    private final ExecutorService executor;
    private volatile boolean started = false;

    private final StatsListener stats = new StatsListener();

    private ThreadedTaskScheduler(int numWorkers) {
        this.numWorkers = numWorkers;

        this.executor = Executors.newFixedThreadPool(numWorkers,new NamedThreadFactory());
    }

    /**
     * Start the workers working. Until this method is called, no threads will be started.
     */
    public void start(){
        synchronized (this){
            if(started) return;
            Worker first = new Worker();
            Worker last = first;
            for(int i=1;i<numWorkers;i++){
                Worker current = new Worker();
                last.next = current;
                last = current;
            }
            last.next = first; //make the list circular
            nextWorker.set(first);

            Worker worker = first;
            for(int i=0;i<numWorkers;i++){
                final Worker w = worker;
                executor.submit(new Runnable() {
                    @Override
                    public void run() {
                        w.work();
                    }
                });
                worker = worker.next;
            }
        }
    }

    /**
     * Terminate the Scheduler, cancelling any outstanding work on any worker threads.
     */
    public void shutdown(){
        executor.shutdownNow();
    }

    @Override
    public TaskFuture submit(T task) throws ExecutionException {
        stats.submittedCount.incrementAndGet();
        boolean success = false;
        Worker worker = null;
        while(!success){
            worker = nextWorker.get();
            Worker next = worker.next;
            success = nextWorker.compareAndSet(worker,next);
        }
        stats.numPending.incrementAndGet();
        task.getTaskStatus().attachListener(stats);
        return worker.addTask(task);
    }

    /**
     * Create a new TaskScheduler with the specified number of workers.
     *
     * @param numWorkers the number of worker threads to use
     * @param <T> the type of TaskScheduler to create
     * @return a ThreadedTaskScheduler with the specified number of workers.
     */
    public static <T extends Task> ThreadedTaskScheduler<T> create(int numWorkers) {
        return new ThreadedTaskScheduler<T>(numWorkers);
    }

    @Override
    public int getNumPendingTasks() {
        return stats.numPending.get();
    }

    @Override
    public int getNumWorkers() {
        return numWorkers; //TODO -sf- make this JMX manageable
    }

    @Override
    public long getTotalSubmittedTasks() {
        return stats.submittedCount.get();
    }

    @Override
    public long getTotalCompletedTasks() {
        return stats.completedCount.get();
    }

    @Override
    public long getTotalFailedTasks() {
        return stats.failedCount.get();
    }

    @Override
    public long getTotalCancelledTasks() {
        return stats.cancelledCount.get();
    }

    @Override
    public long getTotalInvalidatedTasks() {
        return stats.invalidatedCount.get();
    }

    @Override
    public int getNumRunningTasks() {
        return stats.numExecuting.get();
    }

    @Override
    public int getHighestWorkerLoad() {
        Worker start = nextWorker.get();
        int maxWorkerLoad = start.tasks.size();
        Worker next = start.next;
        while(next!=start){
            int size = next.tasks.size();
            if(maxWorkerLoad < size)
                maxWorkerLoad = size;
            next = next.next;
        }
        return maxWorkerLoad;
    }

    @Override
    public int getLowestWorkerLoad() {
        Worker start = nextWorker.get();
        int minWorkerLoad = start.tasks.size();
        Worker next = start.next;
        while(next!=start){
            int size = next.tasks.size();
            if(minWorkerLoad > size)
                minWorkerLoad = size;
            next = next.next;
        }
        return minWorkerLoad;
    }

    /*********************************************************************************************************************/
    /*private helper methods*/

    private class StatsListener implements TaskStatus.StatusListener{
        /*Statistics for management and monitoring*/
        private final AtomicLong submittedCount = new AtomicLong(0l);
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


    /*
     * Worker abstraction. Does the actual Task execution in a big loop
     */
    private static class Worker{
        private BlockingDeque<Task> tasks;
        private Worker next;

        private Worker() {
            tasks=  new LinkedBlockingDeque<Task>();
        }

        void work() {
            //find the nearest empty queue, stealing off the back of others if necessary
            //to find work
            while(!Thread.currentThread().isInterrupted()){
                SpliceLogUtils.trace(LOG,"Looking for next Task to execute");
                Task nextTask;
                try {
                    nextTask = getNextTask();
                } catch (InterruptedException e) {
                    //we've been interrupted while waiting for a task, so time to bail
                    break;
                }
                /*
                 * At this point we can guarantee that a task has been found, because one of two things happened:
                 * 1. We stole a work item off of another worker using workerToRead.tasks.pollLast()
                 * 2. We received an item on our own task queue that was non-null
                 *
                 * However, just because we have a task, doesn't mean it wasn't cancelled between being submitted
                 * and now, so we'll need to check it's cancellation status
                 */
                try{
                    if(nextTask.isCancelled()) {
                        SpliceLogUtils.trace(LOG,"task has been cancelled already, no need to execute it");
                        //this task has been cancelled! time to try again
                        continue;
                    }else if(nextTask.isInvalidated()){
                        SpliceLogUtils.trace(LOG,"Task has been invalidated, skipping execution");
                        continue;
                    }
                } catch (ExecutionException e) {
                    LOG.error("task "+ nextTask.toString()+"had an unexpected error while checking cancellation",e.getCause());
                    continue;
                }

                //we have a task that is ready to be executed. Let's go!
                SpliceLogUtils.trace(LOG,"Executing task");
                try{
                    SpliceLogUtils.trace(LOG,"Marking task as executing");
                    nextTask.markStarted();

                    try{
                        SpliceLogUtils.trace(LOG,"Executing task");
                        nextTask.execute();
                        SpliceLogUtils.trace(LOG,"Task executed successfully");
                    }catch(ExecutionException ee){
                        SpliceLogUtils.error(LOG,"Unexpected error executing task",ee.getCause());
                        nextTask.markFailed(ee.getCause());
                        continue;
                    }catch(InterruptedException ie){
                        SpliceLogUtils.info(LOG,"Interrupted while executing task, cancelling task and continuing");
                        //if we receive and interrupted exception while processing, that indicates that we've
                        //been cancelled in some way
                        //so make sure the task is marked, and then cycle back to the next task
                        nextTask.markCancelled();
                        //clear the interruption flag because an interruption here indicates that
                        //the task needed to be cancelled
                        Thread.interrupted();
                        continue;
                    }
                    //we made it!
                    nextTask.markCompleted();

                    SpliceLogUtils.trace(LOG,"Task has completed successfully");
                }catch(CancellationException ce){
                    SpliceLogUtils.info(LOG,"task %s was cancelled",nextTask.getTaskId());
                }catch(ExecutionException ee){
                    LOG.error("task "+ nextTask.toString()+" had an unexpected error during processing",ee.getCause());
                    try {
                        nextTask.markFailed(ee.getCause());
                    } catch (ExecutionException e) {
                        LOG.error("task " + nextTask.toString() + " had an unexpected error marking failed", ee.getCause());
                    }
                }catch(Throwable t){
                    LOG.error("task "+ nextTask.toString()+" had an unexpected error during processing",t);
                    try {
                        nextTask.markFailed(t);
                    } catch (Throwable e) {
                        LOG.error("task " + nextTask.toString() + " had an unexpected error marking failed", e);
                    }
                }
            }
        }

        private Task getNextTask() throws InterruptedException {
            Task nextTask = tasks.poll();
            Worker workerToRead = this;
            while(nextTask==null){
                SpliceLogUtils.trace(LOG, "No Tasks on my queue, trying to steal from others");
            /*
             * We don't have any items, so try and steal a task off the back of someone else
             */
                workerToRead = workerToRead.next;
                if(workerToRead==this){
                    SpliceLogUtils.trace(LOG,"No Tasks available on any queue, waiting for one to be distributed to me");
                    //we've gone through the entire list of workers available without finding
                    //anything to do, so just wait on our own queue for the next task
                    nextTask = tasks.take();
                }else{
                    SpliceLogUtils.trace(LOG,"Attempting to get task from other worker");
                    nextTask = workerToRead.tasks.pollLast();
                }
            }
            return nextTask;
        }

        public TaskFuture addTask(Task task) {
            ThreadTaskFuture future = new ThreadTaskFuture(task,tasks.size()+1);
            task.getTaskStatus().attachListener(future);
            tasks.offer(task);

            return future;
        }
    }

    private static class ThreadTaskFuture implements TaskFuture, TaskStatus.StatusListener {
        private volatile Task task;
        private final double cost;
        private volatile Thread executingThread;

        private ThreadTaskFuture(Task task,double cost) {
            this.task = task;
            this.cost = cost;
        }

        @Override
        public Status getStatus() throws ExecutionException {
            return task.getTaskStatus().getStatus();
        }

        @Override
        public void complete() throws ExecutionException, CancellationException, InterruptedException {
            while(true){
                switch (task.getTaskStatus().getStatus()) {
                    case FAILED:
                        throw new ExecutionException(task.getTaskStatus().getError());
                    case CANCELLED:
                        throw new CancellationException();
                    case COMPLETED:
                        return;
                }
                synchronized (task){
                    task.wait();
                }
            }
        }

        @Override
        public double getEstimatedCost() {
            return cost;
        }

        @Override
        public void cancel() throws ExecutionException {
            task.markCancelled();
        }

        @Override
        public String getTaskId() {
            return task.getTaskId();
        }

        @Override
        public void statusChanged(Status oldStatus, Status newStatus,TaskStatus taskStatus) {
            /*
             * If newStatus = EXECUTING, then task just began. Set self
             * up to watch for cancellation or failure/completion
             * if newStatus = CANCELLED, then task was cancelled. interrupt
             * the running thread if necessary, and notify anyone waiting
             * to check their state again
             * if newStatus = FAILED, then task failed. Notify anyone waiting
             * to check their state again
             * if newStatus = COMPLETED, then task is finished. Notify anyone waiting
             * to check their state again
             */
            switch (newStatus) {
                case EXECUTING:
                    executingThread = Thread.currentThread();
                    return;
                case CANCELLED:
                    if(executingThread!=null){
                        executingThread.interrupt();
                    }
                    //fall through here to ensure notification of waiting threads
                case FAILED:
                case COMPLETED:
                    synchronized (task){
                        task.notifyAll();
                    }
            }
        }
    }

    private static class NamedThreadFactory implements ThreadFactory {
        private final AtomicInteger threadCount = new AtomicInteger(0);
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("taskScheduler-workerThread-"+threadCount);
            t.setUncaughtExceptionHandler(uncaughtErrorHandler);
            return t;
        }
    }

    private static final Thread.UncaughtExceptionHandler uncaughtErrorHandler = new Thread.UncaughtExceptionHandler() {
        @Override
        public void uncaughtException(Thread t, Throwable e) {
            SpliceLogUtils.error(LOG,"Unexpected, uncaught exception thrown on thread "+t.getName()+", will likely cause deadlocks",e);
        }
    };
}
