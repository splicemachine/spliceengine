package com.splicemachine.derby.impl.job.coprocessor;

import com.google.common.base.Throwables;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.table.IncorrectRegionException;
import com.splicemachine.job.Status;
import com.splicemachine.job.TaskFuture;
import com.splicemachine.job.TaskScheduler;
import com.splicemachine.job.TaskStatus;
import com.splicemachine.utils.SingletonSortedSet;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.ZkUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;
import java.util.SortedSet;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class CoprocessorTaskScheduler extends BaseEndpointCoprocessor implements SpliceSchedulerProtocol{
    private static final Logger LOG = Logger.getLogger(CoprocessorTaskScheduler.class);
    private TaskScheduler<RegionTask> taskScheduler;
    private Set<RegionTask> runningTasks;

		private TaskSplitter splitter;

    @Override
    public void start(CoprocessorEnvironment env) {
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)env;
        HRegion region = rce.getRegion();
//				splitter = NoOpTaskSplitter.INSTANCE;
				splitter = new StoreFileTaskSplitter(region,
								SpliceConstants.maxInterRegionTaskSplits,
								SpliceConstants.interRegionTaskSplitThresholdBytes);
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
                        Bytes.toLong(task.getTaskId())+", corresponding job may fail",e.getCause());
            }
        }
        runningTasks.clear();
        SpliceDriver.driver().getTaskMonitor().deregisterRegion(((RegionCoprocessorEnvironment)env).getRegion().getRegionNameAsString());

        super.stop(env);
    }

    @Override
    public TaskFutureContext[] submit(byte[] taskStart,byte[] taskEnd,final RegionTask task) throws IOException {
        RegionCoprocessorEnvironment rce = (RegionCoprocessorEnvironment)this.getEnvironment();

        //make sure that the task is fully contained within this region
        if(!HRegionUtil.containsRange(rce.getRegion(),taskStart,taskEnd))
            throw new IncorrectRegionException("Incorrect region for Task submission");

				Set<SizedInterval> splitPoints = split(task,taskStart,taskEnd);
				return submitAll(task, splitPoints, rce);
    }

		@SuppressWarnings("unchecked")
		private SortedSet<SizedInterval> split(RegionTask task, byte[] taskStart, byte[] taskEnd) throws IOException {
				if(!task.isSplittable())
						return new SingletonSortedSet<SizedInterval>(new SizedInterval(taskStart, taskEnd,0));

				return splitter.split(task,taskStart,taskEnd);
		}

		private TaskFutureContext[] submitAll(final RegionTask task,Collection<SizedInterval> splitPoints,RegionCoprocessorEnvironment rce) throws IOException{
				TaskFutureContext[] all = new TaskFutureContext[splitPoints.size()];

				if(splitPoints.size()==1){
						//noinspection LoopStatementThatDoesntLoop
						for(SizedInterval split:splitPoints){
								all[0] = doSubmit(task,split.startKey,split.endKey,rce);
								return all;
						}
				}
				int i=0;
				for(SizedInterval split:splitPoints){
						all[i] = doSubmit(task.getClone(),split.startKey,split.endKey,rce);
						i++;
				}

				return all;
		}

    private TaskFutureContext doSubmit(final RegionTask task,byte[] start, byte[] stop,RegionCoprocessorEnvironment rce) throws IOException {
        try {
            //prepare the task for this specific region
            task.prepareTask(start,stop,rce,ZkUtils.getZkManager());

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
														if(LOG.isTraceEnabled())
																LOG.trace("Removing task "+Bytes.toLong(task.getTaskId())+" from running task list");

														runningTasks.remove(task);
														status.detachListener(this);
                            break;
                    }
                }
            });

            TaskFuture future = taskScheduler.submit(task);
            return new TaskFutureContext(task.getTaskNode(),start,future.getTaskId(),future.getEstimatedCost());
        } catch (ExecutionException e) {
            Throwable t = Throwables.getRootCause(e);
            throw Exceptions.getIOException(t);
        }
    }

    public static String getJobPath() {
        return SpliceUtils.zkSpliceJobPath;
    }

}
