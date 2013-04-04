package com.splicemachine.derby.impl.job.scheduler;

import com.splicemachine.job.Status;
import com.splicemachine.job.Task;
import com.splicemachine.job.TaskFuture;
import com.splicemachine.job.TaskScheduler;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
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
public class  ThreadedTaskScheduler<T extends Task> implements TaskScheduler<T> {
    private static final Logger LOG = Logger.getLogger(ThreadedTaskScheduler.class);
    private final int numWorkers;
    private AtomicReference< Worker> nextWorker = new AtomicReference<Worker>();
    private final ExecutorService executor;
    private volatile boolean started = false;

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
        boolean success = false;
        Worker worker = null;
        while(!success){
            worker = nextWorker.get();
            Worker next = worker.next;
            success = nextWorker.compareAndSet(worker,next);
        }
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

    /*********************************************************************************************************************/
    /*private helper methods*/

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
                        continue;
                    }
                    //we made it!
                    nextTask.markCompleted();

                    SpliceLogUtils.trace(LOG,"Task has completed successfully");
                }catch(ExecutionException ee){
                    LOG.error("task "+ nextTask.toString()+" had an unexpected error during processing",ee.getCause());
                    try {
                        nextTask.markFailed(ee.getCause());
                    } catch (ExecutionException e) {
                        LOG.error("task " + nextTask.toString() + " had an unexpected error marking failed", ee.getCause());
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
            WrappedTask wTask = new WrappedTask(task,new ThreadTaskFuture(task.getTaskId(),tasks.size()+1));
            tasks.offer(wTask);

            return wTask.taskFuture;
        }

    }

    /*
     * Makes sure that the returned Future and the underlying Task keep their state in sync properly
     */
    private static class WrappedTask implements Task{
        private Task delegate;
        private ThreadTaskFuture taskFuture;
        private Thread executingThread;

        public WrappedTask(Task task, ThreadTaskFuture threadTaskFuture) {
            this.delegate = task;
            this.taskFuture = threadTaskFuture;
            taskFuture.task = this;
        }

        @Override
        public void markStarted() throws ExecutionException, CancellationException {
            taskFuture.setStatus(Status.EXECUTING);
            executingThread = Thread.currentThread();
            delegate.markStarted();
        }

        @Override
        public void markCompleted() throws ExecutionException {
            taskFuture.setStatus(Status.COMPLETED);
            delegate.markCompleted();
        }

        @Override
        public void markFailed(Throwable error) throws ExecutionException {
            taskFuture.setStatus(Status.FAILED);
            taskFuture.setError(error);
            delegate.markFailed(error);
        }

        @Override
        public void markCancelled() throws ExecutionException {
            taskFuture.setStatus(Status.CANCELLED);
            executingThread.interrupt();
            delegate.markCancelled();
        }

        @Override
        public void execute() throws ExecutionException, InterruptedException {
            try{
                delegate.execute();
            }catch(RuntimeException e){
                throw new ExecutionException(e);
            }
        }

        @Override
        public boolean isCancelled() throws ExecutionException {
            boolean cancelled = delegate.isCancelled();
            if(cancelled){
                taskFuture.setStatus(Status.CANCELLED);
            }
            return cancelled;
        }

        @Override
        public String getTaskId() {
            return delegate.getTaskId();
        }
    }


    private static class ThreadTaskFuture implements TaskFuture {
        private volatile Status status = Status.PENDING;
        private volatile Throwable error;
        private final double cost;
        private final String taskId;
        private volatile WrappedTask task;

        private ThreadTaskFuture(String taskId,double cost) {
            this.cost = cost;
            this.taskId = taskId;
        }

        @Override
        public Status getStatus() throws ExecutionException {
            return status;
        }

        @Override
        public void complete() throws ExecutionException, CancellationException, InterruptedException {
            synchronized (taskId){
                while(true){
                    switch (status) {
                        case FAILED:
                            throw new ExecutionException(error);
                        case CANCELLED:
                            throw new CancellationException();
                        case COMPLETED:
                            return;
                    }
                    taskId.wait();
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
            return taskId;
        }

        public void setStatus(Status status) {
            this.status = status;
            switch (status) {
                case FAILED:
                case COMPLETED:
                case CANCELLED:
                    synchronized (taskId){
                        taskId.notifyAll();
                    }
            }
        }

        public void setError(Throwable error) {
            this.error = error;
        }
    }

    private class NamedThreadFactory implements ThreadFactory {
        private final AtomicInteger threadCount = new AtomicInteger(0);
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("taskScheduler-workerThread-"+threadCount);
            //TODO -sf- set an uncaught exception handler
            return t;
        }
    }
}
