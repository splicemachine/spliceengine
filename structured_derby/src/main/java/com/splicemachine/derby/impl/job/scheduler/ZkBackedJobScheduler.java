package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.splicemachine.derby.impl.job.coprocessor.TaskFutureContext;
import com.splicemachine.derby.impl.job.coprocessor.TaskStatus;
import com.splicemachine.job.*;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectInputStream;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public abstract class ZkBackedJobScheduler<J extends Job> implements JobScheduler<J>{
    protected final RecoverableZooKeeper zooKeeper;

    public ZkBackedJobScheduler(RecoverableZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
    }

    @Override
    public JobFuture submit(final J job) throws ExecutionException {
        try {
            WatchingFuture future = new WatchingFuture(submitTasks(job));
            future.attachWatchers();
            return future;
        } catch (Throwable throwable) {
            Throwable root = Throwables.getRootCause(throwable);
            throw new ExecutionException(root);
        }
    }

    @Override
    public void cleanupJob(JobFuture future) throws ExecutionException {
        //make sure that we CAN clean up this job
        Preconditions.checkArgument(WatchingFuture.class.isAssignableFrom(future.getClass()),"unknown JobFuture type: "+ future.getClass());

        WatchingFuture futureToClean = (WatchingFuture)future;
        futureToClean.cleanup();
    }

    protected abstract Set<WatchingTask> submitTasks(J job) throws ExecutionException;

    protected class WatchingFuture implements JobFuture{
        private final Collection<WatchingTask> taskFutures;
        private final BlockingQueue<TaskFuture> changedFutures;
        private final Set<TaskFuture> completedFutures;
        private final Set<TaskFuture> failedFutures;
        private final Set<TaskFuture> cancelledFutures;

        private volatile Status currentStatus = Status.PENDING;

        private WatchingFuture(Collection<WatchingTask> taskFutures) {
            this.taskFutures = taskFutures;

            this.changedFutures = new LinkedBlockingQueue<TaskFuture>();
            this.completedFutures = Collections.newSetFromMap(new ConcurrentHashMap<TaskFuture, Boolean>());
            this.failedFutures = Collections.newSetFromMap(new ConcurrentHashMap<TaskFuture, Boolean>());
            this.cancelledFutures = Collections.newSetFromMap(new ConcurrentHashMap<TaskFuture, Boolean>());
        }

        private void attachWatchers() throws ExecutionException {
            for(WatchingTask taskFuture:taskFutures){
                taskFuture.attachJobFuture(this);
                /*
                 * We know, because we know the type of the TaskFuture, that calling getStatus()
                 * will actually attach a watcher for us, so we don't have to worry about reattaching.
                 */
                Status status = taskFuture.getStatus();
                switch (status) {
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

                found = !completedFutures.contains(changedFuture) &&
                        !failedFutures.contains(changedFuture) &&
                        !cancelledFutures.contains(changedFuture);
                futuresRemaining = getOutstandingCount();

                if(found){
                    Status status = changedFuture.getStatus();
                    switch (status) {
                        case FAILED:
                            failedFutures.add(changedFuture);
                            changedFuture.complete(); //will throw an ExecutionException immediately
                            break;
                        case COMPLETED:
                            completedFutures.add(changedFuture); //found the next completed task
                            return;
                        case CANCELLED:
                            cancelledFutures.add(changedFuture);
                            throw new CancellationException();
                        default:
                            found=false; //circle around because we aren't finished yet
                    }
                }
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
        public int getNumTasks() throws ExecutionException {
            return taskFutures.size();
        }

        @Override
        public int getNumCompletedTasks() throws ExecutionException {
            return completedFutures.size();
        }

        @Override
        public int getNumFailedTasks() throws ExecutionException {
            return failedFutures.size();
        }

        @Override
        public int getNumCancelledTasks() throws ExecutionException {
            return cancelledFutures.size();
        }

        public void cleanup() throws ExecutionException{
            for(WatchingTask task:taskFutures){
               task.cleanup();
            }
        }
    }

    protected class WatchingTask implements TaskFuture,Watcher {
        private final TaskFutureContext context;
        private final RecoverableZooKeeper zooKeeper;
        private WatchingFuture jobFuture;

        private volatile TaskStatus status = new TaskStatus(Status.PENDING,null);
        private volatile boolean refresh = true;

        public WatchingTask(TaskFutureContext result,RecoverableZooKeeper zooKeeper) {
            this.context = result;
            this.zooKeeper = zooKeeper;
        }

        void attachJobFuture(WatchingFuture jobFuture){
            this.jobFuture = jobFuture;
        }

        @Override
        public Status getStatus() throws ExecutionException {
            //these three status are permanent--once they have been entered, they cannot be escaped
            //so no reason to refresh even if fired (which it should never be)
            switch (status.getStatus()) {
                case COMPLETED:
                case FAILED:
                case CANCELLED:
                   return status.getStatus();
            }
            if(!refresh) return status.getStatus();
            else{
                try {
                    byte[] data = zooKeeper.getData(context.getTaskNode()+"/status",this,new Stat());
                    ByteArrayInputStream bais = new ByteArrayInputStream(data);
                    ObjectInput in = new ObjectInputStream(bais);
                    status = (TaskStatus)in.readObject();
                } catch (KeeperException e) {
                    if(e.code() == KeeperException.Code.NONODE){
                        status = TaskStatus.cancelled();
                    }else
                        throw new ExecutionException(e);
                } catch (InterruptedException e) {
                    throw new ExecutionException(e);
                } catch (IOException e) {
                    throw new ExecutionException(e);
                } catch (ClassNotFoundException e) {
                    throw new ExecutionException(e);
                }

                refresh=false;
                return status.getStatus();
            }
        }

        @Override
        public void complete() throws ExecutionException, CancellationException, InterruptedException {
            while(true){
                Status runningStatus = getStatus();
                switch (runningStatus) {
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

        @Override
        public double getEstimatedCost() {
            return context.getEstimatedCost();
        }

        @Override
        public void process(WatchedEvent event) {
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
                    return;
            }
            status = TaskStatus.cancelled();
            /*
             * Delete the status node for this task to signal that we've cancelled the task
             */
//            try {
//                zooKeeper.delete(context.getTaskNode()+"/status",-1);
//            } catch (InterruptedException e) {
//                throw new ExecutionException(e);
//            } catch (KeeperException e) {
//                if(e.code()== KeeperException.Code.NONODE){
//                    //ignore, cause it's already been removed
//                }
//                throw new ExecutionException(e);
//            }
        }

        @Override
        public String getTaskId() {
            return context.getTaskNode();
        }

        public void cleanup() throws ExecutionException {
            try{
            zooKeeper.delete(context.getTaskNode()+"/status",-1);
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
