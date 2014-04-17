package com.splicemachine.derby.impl.job.coprocessor;

import com.google.common.base.Throwables;
import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.table.IncorrectRegionException;
import com.splicemachine.job.Status;
import com.splicemachine.job.TaskFuture;
import com.splicemachine.job.TaskScheduler;
import com.splicemachine.job.TaskStatus;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.ZkUtils;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.BaseEndpointCoprocessor;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
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
				splitter = new StoreFileTaskSplitter(region,8,32*1024*1024l);
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

				List<Pair<byte[],byte[]>> splitPoints = split(task,taskStart,taskEnd);
				return submitAll(task, splitPoints, rce);
    }

		@SuppressWarnings("unchecked")
		private List<Pair<byte[], byte[]>> split(RegionTask task, byte[] taskStart, byte[] taskEnd) throws IOException {
				if(!task.isSplittable())
						return Collections.singletonList(Pair.newPair(taskStart, taskEnd));

				List<byte[]> splitPoints = splitter.split(task,taskStart,taskEnd);
				if(splitPoints.size()<=0)
						return Collections.singletonList(Pair.newPair(taskStart, taskEnd));

				List<Pair<byte[],byte[]>> splits = Lists.newArrayListWithCapacity(splitPoints.size()-1);
				byte[] splitStart = taskStart;
				for (byte[] splitEnd : splitPoints) {
						splits.add(Pair.newPair(splitStart, splitEnd));
						splitStart = splitEnd;
				}

				splits.add(Pair.newPair(splitStart, taskEnd));
				return splits;
		}

		private TaskFutureContext[] submitAll(final RegionTask task,List<Pair<byte[],byte[]>> splitPoints,RegionCoprocessorEnvironment rce) throws IOException{
				TaskFutureContext[] all = new TaskFutureContext[splitPoints.size()];

				if(splitPoints.size()==1){
						all[0] = doSubmit(task,splitPoints.get(0).getFirst(),splitPoints.get(0).getSecond(),rce);
						return all;
				}
				int i=0;
				for(Pair<byte[],byte[]> split:splitPoints){
						all[i] = doSubmit(task.getClone(),split.getFirst(),split.getSecond(),rce);
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
