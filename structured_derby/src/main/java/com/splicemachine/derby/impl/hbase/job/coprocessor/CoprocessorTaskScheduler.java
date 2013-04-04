package com.splicemachine.derby.impl.hbase.job.coprocessor;

import com.google.common.base.Throwables;
import com.splicemachine.derby.hbase.job.TaskFuture;
import com.splicemachine.derby.hbase.job.TaskScheduler;
import com.splicemachine.derby.impl.hbase.job.OperationJob;
import com.splicemachine.derby.utils.Exceptions;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.zookeeper.RecoverableZooKeeper;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class CoprocessorTaskScheduler extends BaseEndpointCoprocessor implements SpliceSchedulerProtocol{
    private TaskScheduler<SinkTask> taskScheduler;
    private RecoverableZooKeeper zooKeeper;

    @Override
    public TaskFutureContext submit(OperationJob job) throws IOException {
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)this.getEnvironment();
        try {
            TaskFuture future = taskScheduler.submit(new SinkTask(job,zooKeeper,rce.getRegion()));
            return new TaskFutureContext(future.getTaskId(),future.getEstimatedCost());
        } catch (ExecutionException e) {
            Throwable t = Throwables.getRootCause(e);
            throw Exceptions.getIOException(t);
        }
    }
}
