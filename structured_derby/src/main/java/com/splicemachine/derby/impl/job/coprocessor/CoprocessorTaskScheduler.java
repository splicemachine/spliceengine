package com.splicemachine.derby.impl.job.coprocessor;

import com.google.common.base.Throwables;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.scheduler.ZkBackedTaskScheduler;
import com.splicemachine.derby.utils.ZkUtils;
import com.splicemachine.job.TaskFuture;
import com.splicemachine.job.TaskScheduler;
import com.splicemachine.derby.impl.job.OperationJob;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.si2.data.hbase.HbRegion;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class CoprocessorTaskScheduler extends BaseEndpointCoprocessor implements SpliceSchedulerProtocol{
    private static final String DEFAULT_BASE_TASK_QUEUE_NODE = "/spliceTasks";
    private TaskScheduler<SinkTask> taskScheduler;
    private RecoverableZooKeeper zooKeeper;
    private volatile String baseTaskQueueNode;

    @Override
    public void start(CoprocessorEnvironment env) {
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)env;
        zooKeeper = rce.getRegionServerServices().getZooKeeper().getRecoverableZooKeeper();
        Configuration configuration = env.getConfiguration();

        baseTaskQueueNode = configuration.get("splice.sink.baseTaskQueueNode",DEFAULT_BASE_TASK_QUEUE_NODE);
        try {

            HRegion region = rce.getRegion();
            String regionQueueNode = baseTaskQueueNode+"/"+region.getTableDesc().getNameAsString()+"/"+region.getRegionNameAsString();
            ZkUtils.recursiveSafeCreate(regionQueueNode,new byte[]{},ZooDefs.Ids.OPEN_ACL_UNSAFE,CreateMode.PERSISTENT);
        } catch (KeeperException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        taskScheduler = new ZkBackedTaskScheduler<SinkTask>(zooKeeper, SpliceDriver.driver().getTaskScheduler());

        //TODO -sf- read the existing tasks for the region, and schedule them
        super.start(env);
    }

    @Override
    public void stop(CoprocessorEnvironment env) {
        //TODO -sf- remove tasks for the region from the scheduler WITHOUT cancelling them

        //TODO -sf- deal with region splits!
        super.stop(env);
    }

    @Override
    public TaskFutureContext submit(OperationJob job) throws IOException {
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)this.getEnvironment();
        try {
            TaskFuture future = taskScheduler.submit(new SinkTask(job,zooKeeper,rce.getRegion(),baseTaskQueueNode));
            return new TaskFutureContext(future.getTaskId(),future.getEstimatedCost());
        } catch (ExecutionException e) {
            Throwable t = Throwables.getRootCause(e);
            throw Exceptions.getIOException(t);
        }
    }
}
