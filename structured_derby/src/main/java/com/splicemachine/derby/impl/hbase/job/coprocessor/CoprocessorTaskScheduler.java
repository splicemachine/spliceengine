package com.splicemachine.derby.impl.hbase.job.coprocessor;

import com.splicemachine.derby.hbase.job.Task;
import com.splicemachine.derby.hbase.job.TaskFuture;
import com.splicemachine.derby.hbase.job.TaskScheduler;
import com.splicemachine.derby.impl.hbase.job.OperationJob;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class CoprocessorTaskScheduler extends BaseEndpointCoprocessor implements SpliceSchedulerProtocol,TaskScheduler{

    @Override
    public TaskFutureContext submit(OperationJob job) throws IOException {
        return null;
    }

    @Override
    public TaskFuture submit(Task task) throws ExecutionException {
        return null;
    }

}
