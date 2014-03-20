package com.splicemachine.derby.impl.job.coprocessor;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.base.Throwables;
import com.google.protobuf.ByteString;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.job.*;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest;
import com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerResponse;
import com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerService;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.table.IncorrectRegionException;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.ZkUtils;

/**
 * @author Scott Fines
 *         Created on: 4/3/13
 */
public class CoprocessorTaskScheduler extends SpliceSchedulerService implements CoprocessorService, Coprocessor {
    private static final Logger LOG = Logger.getLogger(CoprocessorTaskScheduler.class);
    RegionCoprocessorEnvironment rce;
    private TaskScheduler<RegionTask> taskScheduler;
    private Set<RegionTask> runningTasks;

    public static String getJobPath() {
        return SpliceUtils.zkSpliceJobPath;
    }

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        rce = (RegionCoprocessorEnvironment) env;
        HRegion region = rce.getRegion();
        runningTasks = SpliceDriver.driver().getTaskMonitor().registerRegion(region.getRegionInfo()
                                                                                   .getRegionNameAsString());
        taskScheduler = SpliceDriver.driver().getTaskScheduler();
    }

    @Override
    public void stop(CoprocessorEnvironment env) throws IOException {
        /*
         * We are stopping. Either The region is moving to a different region, or it's splitting. Either way,
         * we need to resubmit our tasks to the correct locations.
         */
        for (RegionTask task : runningTasks) {
            try {
                if (task.invalidateOnClose())
                    task.markInvalid();
            } catch (ExecutionException e) {
                SpliceLogUtils.error(LOG, "Unexpected error invalidating task " +
                        Bytes.toLong(task.getTaskId()) + ", corresponding job may fail", e.getCause());
            }
        }
        runningTasks.clear();
        SpliceDriver.driver().getTaskMonitor().deregisterRegion(((RegionCoprocessorEnvironment) env).getRegion()
                                                                                                    .getRegionNameAsString());

    }

		@Override
		public void submit(RpcController rpcController, SpliceSchedulerRequest spliceSchedulerRequest,
											 RpcCallback<SpliceSchedulerResponse> spliceSchedulerResponse) {

				try {
						byte[] start = spliceSchedulerRequest.getTaskStart().toByteArray();
						byte[] end = spliceSchedulerRequest.getTaskEnd().toByteArray();
						if(!HRegionUtil.containsRange(rce.getRegion(),start,end)){
								ResponseConverter.setControllerException(rpcController,new IncorrectRegionException("incorrect region for task submission"));
								return;
						}

						byte[] taskBytes = spliceSchedulerRequest.getClassBytes().toByteArray();
						Input input = new Input(taskBytes);
						Kryo kryo = SpliceKryoRegistry.getInstance().get();
						RegionTask t;
						try{
								t = (RegionTask)kryo.readClassAndObject(input);
						}finally{
								SpliceKryoRegistry.getInstance().returnInstance(kryo);
						}
						TaskFutureContext context = doSubmit(t,rce);
						SpliceSchedulerResponse response = SpliceSchedulerResponse.newBuilder().setTaskId(ByteString.copyFrom(context.getTaskId()))
										.setTaskNode(context.getTaskNode())
										.setEstimatedCost(context.getEstimatedCost())
										.build();
						spliceSchedulerResponse.run(response);
				} catch (IOException e) {
						ResponseConverter.setControllerException(rpcController,e);
				}
		}

    public TaskFutureContext submit(byte[] taskStart, byte[] taskEnd, final RegionTask task) throws IOException {
        //make sure that the task is fully contained within this region
        if (!HRegionUtil.containsRange(rce.getRegion(), taskStart, taskEnd))
            throw new IncorrectRegionException("Incorrect region for Task submission");

        return doSubmit(task, rce);
    }

    private TaskFutureContext doSubmit(final RegionTask task, RegionCoprocessorEnvironment rce) throws IOException {
        try {
            //prepare the task for this specific region
            task.prepareTask(rce, ZkUtils.getZkManager());

            runningTasks.add(task);
            /*
             * Here we attach a listener so that when the task completes, fails, or otherwise
             * becomes invalid, we can remove it from our Region set and thus avoid memory leaks
             */
            task.getTaskStatus().attachListener(new TaskStatus.StatusListener() {
                @Override
                public void statusChanged(Status oldStatus, Status newStatus, TaskStatus status) {
                    switch (newStatus) {
                        case FAILED:
                        case COMPLETED:
                        case CANCELLED:
                            if (LOG.isTraceEnabled())
                                LOG.trace("Removing task " + Bytes.toLong(task.getTaskId()) + " from running task " +
                                                  "list");

                            runningTasks.remove(task);
                            status.detachListener(this);
                            break;
                    }
                }
            });

            TaskFuture future = taskScheduler.submit(task);
            return new TaskFutureContext(task.getTaskNode(), future.getTaskId(), future.getEstimatedCost());
        } catch (ExecutionException e) {
            Throwable t = Throwables.getRootCause(e);
            throw Exceptions.getIOException(t);
        }
    }

	@Override
	public Service getService() {
		return this;
	}


}
