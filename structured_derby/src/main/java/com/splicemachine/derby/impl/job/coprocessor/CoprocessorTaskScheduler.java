package com.splicemachine.derby.impl.job.coprocessor;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.derby.utils.ZkUtils;
import com.splicemachine.job.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class CoprocessorTaskScheduler extends BaseEndpointCoprocessor implements SpliceSchedulerProtocol{
    private static final String DEFAULT_BASE_TASK_QUEUE_NODE = "/spliceTasks";
    private static final Logger LOG = Logger.getLogger(CoprocessorTaskScheduler.class);
    private TaskScheduler<RegionTask> taskScheduler;
    private RecoverableZooKeeper zooKeeper;
    private volatile String baseTaskQueueNode;
    private Set<Task> runningTasks = Collections.newSetFromMap(new ConcurrentHashMap<Task, Boolean>());
    public static String baseQueueNode;

    static{
         baseQueueNode = SpliceUtils.config.get("splice.sink.baseTaskQueueNode",DEFAULT_BASE_TASK_QUEUE_NODE);
    }

    @Override
    public void start(CoprocessorEnvironment env) {
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)env;
        zooKeeper = rce.getRegionServerServices().getZooKeeper().getRecoverableZooKeeper();
        try {
            HRegion region = rce.getRegion();
            String regionQueueNode = baseQueueNode+"/"+region.getTableDesc().getNameAsString()+"/"+region.getRegionNameAsString();
            ZkUtils.recursiveSafeCreate(regionQueueNode,new byte[]{},ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        taskScheduler = SpliceDriver.driver().getTaskScheduler();

        //TODO -sf- read the existing tasks for the region, and schedule them
        super.start(env);
    }

    @Override
    public void stop(CoprocessorEnvironment env) {
        /*
         * We are stopping. Either The region is moving to a different region, or it's splitting. Either way,
         * we need to resubmit our tasks to the correct locations.
         */
        for(Task task:runningTasks){
            try {
                task.markInvalid();
            } catch (ExecutionException e) {
                SpliceLogUtils.error(LOG,"Unexpected error invalidating task "+
                        task.getTaskId()+", corresponding job may fail",e.getCause());
            }
        }
        runningTasks.clear();

        super.stop(env);
    }

    @Override
    public TaskFutureContext submit(final RegionTask task) throws IOException {
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)this.getEnvironment();
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
}
