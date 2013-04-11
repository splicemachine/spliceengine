package com.splicemachine.derby.impl.job.coprocessor;

import com.google.common.base.Throwables;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.ByteDataInput;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.ZkUtils;
import com.splicemachine.job.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class CoprocessorTaskScheduler extends BaseEndpointCoprocessor implements SpliceSchedulerProtocol{
    private static final String DEFAULT_BASE_TASK_QUEUE_NODE = "/spliceTasks";
    private static final String DEFAULT_BASE_JOB_QUEUE_NODE = "/spliceJobs";
    private static final Logger LOG = Logger.getLogger(CoprocessorTaskScheduler.class);
    private TaskScheduler<RegionTask> taskScheduler;
    private RecoverableZooKeeper zooKeeper;
    private Set<RegionTask> runningTasks;
    public static String baseQueueNode;
    private static String jobQueueNode;

    private volatile LoadingTask loader;

    static{
         baseQueueNode = SpliceUtils.config.get("splice.sink.baseTaskQueueNode",DEFAULT_BASE_TASK_QUEUE_NODE);
        jobQueueNode = SpliceUtils.config.get("splice.sink.baseJobQueueNode",DEFAULT_BASE_JOB_QUEUE_NODE);
    }

    @Override
    public void start(CoprocessorEnvironment env) {
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)env;
        zooKeeper = rce.getRegionServerServices().getZooKeeper().getRecoverableZooKeeper();
        try {
            HRegion region = rce.getRegion();
            runningTasks = SpliceDriver.driver().getTaskMonitor().registerRegion(region.getRegionInfo().getRegionNameAsString());
            ZkUtils.recursiveSafeCreate(CoprocessorTaskScheduler.baseQueueNode, new byte[]{}, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        taskScheduler = SpliceDriver.driver().getTaskScheduler();
        loader = new LoadingTask();

        //submit a task to load any outstanding tasks from the zookeeper region queue
        try {
            doSubmit(loader,rce);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        super.start(env);
    }

    @Override
    public void stop(CoprocessorEnvironment env) {
        /*
         * We are stopping. Either The region is moving to a different region, or it's splitting. Either way,
         * we need to resubmit our tasks to the correct locations.
         */
        for(RegionTask task:runningTasks){
            try {
                if(task.invalidateOnClose())
                    task.markInvalid();
            } catch (ExecutionException e) {
                SpliceLogUtils.error(LOG,"Unexpected error invalidating task "+
                        task.getTaskId()+", corresponding job may fail",e.getCause());
            }
        }
        runningTasks.clear();
        SpliceDriver.driver().getTaskMonitor().deregisterRegion(((RegionCoprocessorEnvironment)env).getRegion().getRegionNameAsString());

        super.stop(env);
    }

    @Override
    public TaskFutureContext submit(final RegionTask task) throws IOException {
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)this.getEnvironment();
        return doSubmit(task, rce);
    }

    private TaskFutureContext doSubmit(final RegionTask task, RegionCoprocessorEnvironment rce) throws IOException {
        try {
            runningTasks.add(task);
            /*
             * Here we attach a listener so that when the task completes, fails, or otherwise
             * becomes invalid, we can remove it from our Region set and thus avoid memory leaks
             */
            task.getTaskStatus().attachListener(new TaskStatus.StatusListener() {
                @Override
                public void statusChanged(Status oldStatus, Status newStatus,TaskStatus status) {
                    switch (newStatus) {
                        case FAILED:
                        case COMPLETED:
                        case CANCELLED:
                            LOG.trace("Removing task "+task.getTaskId()+" from running task list");
                            runningTasks.remove(task);
                            break;
                    }
                }
            });

            //prepare the task for this specific region
            task.prepareTask(rce.getRegion(),zooKeeper);
            TaskFuture future = taskScheduler.submit(task);
            return new TaskFutureContext(future.getTaskId(),future.getEstimatedCost());
        } catch (ExecutionException e) {
            Throwable t = Throwables.getRootCause(e);
            throw Exceptions.getIOException(t);
        }
    }

    public static String getRegionQueue(HRegionInfo info){
        String queue = CoprocessorTaskScheduler.baseQueueNode+"/"+info.getTableNameAsString();

        byte[] startKey = info.getStartKey();
        if(startKey==null)
            return queue;
        else{
            byte[] regionName = new byte[startKey.length];
            System.arraycopy(startKey,0,regionName,0,startKey.length);
            return queue+"_"+ MD5Hash.getMD5AsHex(regionName);
        }
    }

    public static String getJobPath() {
        return jobQueueNode;
    }

    private class LoadingTask implements RegionTask{
        private HRegion regionToLoad;
        private volatile TaskStatus status = new TaskStatus(Status.PENDING,null);
        private RecoverableZooKeeper zooKeeper;
        private String taskID;


        @Override
        public void markStarted() throws ExecutionException, CancellationException {
            status.setStatus(Status.EXECUTING);
        }

        @Override
        public void markCompleted() throws ExecutionException {
            status.setStatus(Status.COMPLETED);
        }

        @Override
        public void markFailed(Throwable error) throws ExecutionException {
            status.setError(error);
            status.setStatus(Status.FAILED);
        }

        @Override
        public void markCancelled() throws ExecutionException {
            status.setStatus(Status.CANCELLED);
        }

        @Override
        public void execute() throws ExecutionException, InterruptedException {
            //get List of Tasks that are waiting on this region's queue
            String regionQueue = getRegionQueue(regionToLoad.getRegionInfo());
            List<String> tasks;
            try{
                tasks = zooKeeper.getChildren(regionQueue, false);
            } catch (KeeperException e) {
                if(e.code()== KeeperException.Code.NONODE){
                    //probably won't happen, but still
                    //there are obviously no tasks to load, so no worries there
                    return;
                }
                throw new ExecutionException(e);
            }
            for(String taskNode:tasks){
                try{
                    ByteDataInput bdi = new ByteDataInput(zooKeeper.getData(regionQueue+"/"+taskNode,false,new Stat()));
                    Task task = (Task)bdi.readObject();
                    if(task instanceof RegionTask){
                        RegionTask regionTask = (RegionTask)task;
                        regionTask.prepareTask(regionToLoad, zooKeeper);
                        taskScheduler.submit(regionTask);
                    }
                } catch (KeeperException e) {
                    //skip any tasks which were cancelled on us
                    if(e.code()!= KeeperException.Code.NONODE){
                       throw new ExecutionException(e);
                    }
                } catch (ClassNotFoundException e) {
                    throw new ExecutionException(e);
                } catch (IOException e) {
                    throw new ExecutionException(e);
                }
            }
        }

        @Override
        public boolean isCancelled() throws ExecutionException {
            return status.getStatus()==Status.CANCELLED;
        }

        @Override
        public String getTaskId() {
            if(taskID ==null){
                taskID = getRegionQueue(regionToLoad.getRegionInfo())+"/loadingTask";
            }
            return taskID;
        }

        @Override
        public TaskStatus getTaskStatus() {
            return status;
        }

        @Override
        public void markInvalid() throws ExecutionException {
            status.setStatus(Status.INVALID);
        }

        @Override
        public boolean isInvalidated() {
            return status.getStatus()==Status.INVALID;
        }

        @Override
        public void cleanup() throws ExecutionException {
            //no-op
        }

        @Override
        public void prepareTask(HRegion region, RecoverableZooKeeper zooKeeper) throws ExecutionException {
            this.regionToLoad =region;
            this.zooKeeper = zooKeeper;
        }

        @Override
        public boolean invalidateOnClose() {
            return true;
        }
    }
}
