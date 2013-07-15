package com.splicemachine.derby.impl.job.coprocessor;

import com.google.common.base.Throwables;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.*;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.ZkUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Set;
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
            return new TaskFutureContext(task.getTaskNode(),future.getTaskId(),future.getEstimatedCost());
        } catch (ExecutionException e) {
            Throwable t = Throwables.getRootCause(e);
            throw Exceptions.getIOException(t);
        }
    }

    public static String getJobPath() {
        return SpliceUtils.zkSpliceJobPath;
    }

}
