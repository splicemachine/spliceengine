package com.splicemachine.derby.impl.hbase.job.scheduler;

import com.splicemachine.derby.hbase.job.TaskFuture;
import com.splicemachine.derby.hbase.job.TaskScheduler;
import com.splicemachine.derby.impl.hbase.job.coprocessor.OperationTask;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Created on: 4/3/13
 */
public class ZkBackedTaskScheduler implements TaskScheduler<OperationTask> {
    private final RecoverableZooKeeper zooKeeper;
    private final String baseQueueNode;
    private final TaskScheduler<OperationTask> delegate;

    public ZkBackedTaskScheduler(String baseQueueNode, RecoverableZooKeeper zooKeeper, TaskScheduler<OperationTask> delegate) {
        this.zooKeeper = zooKeeper;
        this.baseQueueNode = baseQueueNode;
        this.delegate = delegate;
    }

    @Override
    public TaskFuture submit(OperationTask task) throws ExecutionException {

        try{
            byte[] data = task.getTaskStatus().toBytes();

            String queue = getRegionQueueNode(task.getRegion());

            String taskId = queue+"/task-"+task.getInstructions().getTopOperation().getUniqueSequenceID()+"-";
            taskId = zooKeeper.create(taskId,data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
            task.setTaskId(queue+"/"+taskId);

            return delegate.submit(task);
        } catch (IOException e) {
            throw new ExecutionException(e);
        } catch (InterruptedException e) {
            throw new ExecutionException(e);
        } catch (KeeperException e) {
            throw new ExecutionException(e);
        }
    }

    private String getRegionQueueNode(HRegion region) {
        String tableName = region.getTableDesc().getNameAsString();
        String regionName = region.getRegionNameAsString();

        return baseQueueNode+"/"+tableName+"/"+regionName;
    }
}
