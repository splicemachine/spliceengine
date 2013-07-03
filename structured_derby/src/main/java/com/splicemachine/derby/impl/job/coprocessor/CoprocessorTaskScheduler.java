package com.splicemachine.derby.impl.job.coprocessor;

import com.google.common.base.Throwables;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.ByteDataInput;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.*;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import com.splicemachine.utils.ZkUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.util.MD5Hash;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class CoprocessorTaskScheduler extends BaseEndpointCoprocessor implements SpliceSchedulerProtocol{
    private static final Logger LOG = Logger.getLogger(CoprocessorTaskScheduler.class);
    private TaskScheduler<RegionTask> taskScheduler;
    private Set<RegionTask> runningTasks;

    @Override
    public void start(CoprocessorEnvironment env) {
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)env;
        HRegion region = rce.getRegion();
        runningTasks = SpliceDriver.driver().getTaskMonitor().registerRegion(region.getRegionInfo().getRegionNameAsString());
        taskScheduler = SpliceDriver.driver().getTaskScheduler();
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
            //prepare the task for this specific region
            task.prepareTask(rce,ZkUtils.getZkManager());

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

            TaskFuture future = taskScheduler.submit(task);
            return new TaskFutureContext(future.getTaskId(),future.getEstimatedCost());
        } catch (ExecutionException e) {
            Throwable t = Throwables.getRootCause(e);
            throw Exceptions.getIOException(t);
        }
    }

    public static String getRegionQueue(HRegionInfo info){
        String queue = SpliceUtils.zkSpliceTaskPath+"/"+info.getTableNameAsString();

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
        return SpliceUtils.zkSpliceJobPath;
    }

    private class LoadingTask implements RegionTask{
        private RegionCoprocessorEnvironment regionToLoad;
        private volatile TaskStatus status = new TaskStatus(Status.PENDING,null);
        private SpliceZooKeeperManager zooKeeper;
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
            final String regionQueue = getRegionQueue(regionToLoad.getRegion().getRegionInfo());
            try {
                zooKeeper.execute(new SpliceZooKeeperManager.Command<Void>() {
                    @Override
                    public Void execute(RecoverableZooKeeper zooKeeper) throws InterruptedException, KeeperException {
                        List<String> tasks;
                        try{
                            tasks = zooKeeper.getChildren(regionQueue,false);
                        }catch(KeeperException e){
                            //if the exception is nonode, then there are no tasks to process
                            //shouldn't ever happen, but just in case
                            if(e.code()!= KeeperException.Code.NONODE)
                                throw e;
                            else
                                return null;
                        }

                        for(String taskNode:tasks){
                            TaskStatus taskStatus;
                            ByteDataInput bdi;
                            try{
                                String statusNode = regionQueue+"/"+taskNode+"/status";
                                bdi = new ByteDataInput(zooKeeper.getData(statusNode,false,new Stat()));
                                taskStatus = (TaskStatus)bdi.readObject();
                            }catch(KeeperException ke){
                                SpliceLogUtils.trace(LOG,"No Status node found for task %s, assuming failed task, resubmitting as EXECUTING state",taskNode);
                                if(ke.code()== KeeperException.Code.NONODE){
                                    taskStatus = new TaskStatus(Status.EXECUTING,null);
                                }else
                                    throw ke;
                            } catch (ClassNotFoundException e) {
                                //won't happen
                                throw new RuntimeException(e);
                            } catch (IOException e) {
                                //won't happen
                                throw new RuntimeException(e);
                            }

                            switch (status.getStatus()) {
                                case FAILED:
                                case COMPLETED:
                                case CANCELLED:
                                    SpliceLogUtils.trace(LOG,"Task %s is marked %s, not re-executing",taskNode,status.getStatus());
                                    continue;
                            }
                            SpliceLogUtils.trace(LOG,"Submitting task %s for re-execution",taskNode);

                            bdi =  new ByteDataInput(zooKeeper.getData(regionQueue+"/"+taskNode,false,new Stat()));
                            try {
                                Task task = (Task)bdi.readObject();
                                task.getTaskStatus().setStatus(taskStatus.getStatus());

                                if(task instanceof RegionTask){
                                    RegionTask regionTask = (RegionTask)task;
                                    regionTask.prepareTask(regionToLoad, ZkUtils.getZkManager());
                                    taskScheduler.submit(regionTask);
                                }
                            } catch (ClassNotFoundException e1) {
                                throw new RuntimeException(e1);
                            } catch (IOException e1) {
                                throw new RuntimeException(e1);
                            } catch (ExecutionException e) {
                                throw new RuntimeException(e);
                            }
                        }
                        return null;
                    }
                });
            } catch (KeeperException e) {
                throw new ExecutionException(e);
            } catch(RuntimeException e){
                if(e.getCause() instanceof ExecutionException)
                    throw (ExecutionException)e.getCause();
            }
        }

        @Override
        public boolean isCancelled() throws ExecutionException {
            return status.getStatus()==Status.CANCELLED;
        }

        @Override
        public String getTaskId() {
            if(taskID ==null){
                taskID = getRegionQueue(regionToLoad.getRegion().getRegionInfo())+"/loadingTask";
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
        public int getPriority() {
            return 100; //run this task before anything else
        }

        @Override
        public void prepareTask(RegionCoprocessorEnvironment rce, SpliceZooKeeperManager zooKeeper) throws ExecutionException {
            this.regionToLoad = rce;
            this.zooKeeper = zooKeeper;
        }

        @Override
        public boolean invalidateOnClose() {
            return true;
        }
    }
}
