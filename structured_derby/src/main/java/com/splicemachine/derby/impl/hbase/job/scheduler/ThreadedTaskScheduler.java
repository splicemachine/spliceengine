package com.splicemachine.derby.impl.hbase.job.scheduler;

import com.google.common.util.concurrent.AbstractFuture;
import com.splicemachine.derby.hbase.job.Status;
import com.splicemachine.derby.hbase.job.Task;
import com.splicemachine.derby.hbase.job.TaskFuture;
import com.splicemachine.derby.hbase.job.TaskScheduler;
import org.apache.log4j.Logger;

import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
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

    private static class Worker{
        private BlockingDeque<Task> tasks;
        private Worker next;
        private volatile boolean shutdown = false;

        private Worker() {
            tasks=  new LinkedBlockingDeque<Task>();
        }

        void work() {
            //find the nearest empty queue, stealing off the back of others if necessary
            //to find work
            workLoop:
            while(!shutdown &&!Thread.currentThread().isInterrupted()){
                Task nextTask = tasks.poll();
                Worker workerToRead = this;
                while(nextTask==null){
                    /*
                     * We don't have any items, so try and steal a task off the back of someone else
                     */
                    workerToRead = workerToRead.next;
                    if(workerToRead==this){
                        //we've gone through the entire list of workers available without finding
                        //anything to do, so just wait on our own queue for the next task
                        try{
                            nextTask = tasks.take();
                        }catch(InterruptedException ie){
                            //we've been shutdown, so break from the loop
                            break workLoop;
                        }
                    }else{
                        nextTask = workerToRead.tasks.pollLast();
                    }
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
                        //this task has been cancelled! time to try again
                        continue;
                    }
                } catch (ExecutionException e) {
                    LOG.error("task "+ nextTask.toString()+"had an unexpected error while checking cancellation",e.getCause());
                    continue;
                }

                //we have a task that is ready to be executed. Let's go!
                try{
                    nextTask.markStarted();
                    try{
                        nextTask.execute();
                    }catch(ExecutionException ee){
                        nextTask.markFailed(ee.getCause());
                        continue;
                    }catch(InterruptedException ie){
                        //if we receive and interrupted exception while processing, that indicates that we've
                        //been cancelled in some way
                        //so make sure the task is marked, and then cycle back to the next task
                        nextTask.markCancelled();
                        continue;
                    }
                    //we made it!
                    nextTask.markCompleted();
                }catch(ExecutionException ee){
                    LOG.error("task "+ nextTask.toString()+" had an unexpected error during processing",ee.getCause());
                }
            }
        }

        public TaskFuture addTask(Task task) {
            WrappedTask wTask = new WrappedTask(task,new ThreadTaskFuture(task.getTaskId(),tasks.size()+1));
            tasks.offer(wTask);

            return wTask.taskFuture;
        }

    }

    private static class WrappedTask implements Task{
        private Task delegate;
        private ThreadTaskFuture taskFuture;

        public WrappedTask(Task task, ThreadTaskFuture threadTaskFuture) {
            this.delegate = task;
            this.taskFuture = threadTaskFuture;
        }

        @Override
        public void markStarted() throws ExecutionException, CancellationException {
            taskFuture.setStatus(Status.EXECUTING);
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
            delegate.markCancelled();
        }

        @Override
        public void execute() throws ExecutionException, InterruptedException {
            delegate.execute();
        }

        @Override
        public boolean isCancelled() throws ExecutionException {
            return delegate.isCancelled();
        }

        @Override
        public String getTaskId() {
            return delegate.getTaskId();
        }
    }


    private static class ThreadTaskFuture implements TaskFuture {
        private volatile Status status = Status.PENDING;
        private Watcher futureSync;
        private volatile Throwable error;
        private final double cost;
        private final String taskId;

        private ThreadTaskFuture(String taskId,double cost) {
            futureSync = new Watcher(this);
            this.cost = cost;
            this.taskId = taskId;
        }

        @Override
        public Status getStatus() throws ExecutionException {
            return status;
        }

        @Override
        public void complete() throws ExecutionException, CancellationException, InterruptedException {
            if(error!=null)
                throw new ExecutionException(error);
            futureSync.get();
        }

        @Override
        public double getEstimatedCost() {
            return cost;
        }

        @Override
        public void cancel() throws ExecutionException {
            futureSync.cancel(true);
        }

        @Override
        public String getTaskId() {
            return taskId;
        }

        public void setStatus(Status status) {
            this.status = status;
        }

        public void setError(Throwable error) {
            futureSync.setException(error);
        }
    }

    private static class Watcher extends AbstractFuture<Void>{
        private final ThreadTaskFuture watchingFuture;

        private Watcher(ThreadTaskFuture watchingFuture) {
            this.watchingFuture = watchingFuture;
        }

        @Override
        public boolean isDone() {
            return watchingFuture.status ==Status.COMPLETED;
        }

        @Override
        public boolean isCancelled() {
            return watchingFuture.status ==Status.CANCELLED;
        }

        @Override
        public boolean setException(Throwable throwable) {
            watchingFuture.error = throwable;
            return super.setException(throwable);
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            watchingFuture.status = Status.CANCELLED;
            return super.cancel(mayInterruptIfRunning);
        }
    }

    private class NamedThreadFactory implements ThreadFactory {
        private final AtomicInteger threadCount = new AtomicInteger(0);
        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r);
            t.setName("taskScheduler-workerThread-"+threadCount);
            return t;
        }
    }
}
