package com.splicemachine.derby.impl.job.coprocessor;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.google.common.base.Throwables;
import com.google.protobuf.RpcCallback;
import com.google.protobuf.RpcController;
import com.google.protobuf.Service;
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerRequest;
import com.splicemachine.coprocessor.SpliceMessage.SpliceSchedulerService;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.table.IncorrectRegionException;
import com.splicemachine.job.Status;
import com.splicemachine.job.TaskFuture;
import com.splicemachine.job.TaskScheduler;
import com.splicemachine.job.TaskStatus;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.ZkUtils;

import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.CoprocessorEnvironment;
import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.coprocessor.CoprocessorService;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.ipc.ServerNotRunningYetException;
import org.apache.hadoop.hbase.protobuf.ResponseConverter;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public class CoprocessorTaskScheduler extends SpliceSchedulerService implements CoprocessorService, Coprocessor {
    private static final Logger LOG = Logger.getLogger(CoprocessorTaskScheduler.class);
    RegionCoprocessorEnvironment rce;
    private TaskScheduler<RegionTask> taskScheduler;
    private Set<RegionTask> runningTasks;

    public static String getJobPath() {
        return SpliceUtils.zkSpliceJobPath;
    }

    private TaskSplitter splitter;
		private String tableName;

    @Override
    public void start(CoprocessorEnvironment env) throws IOException {
        rce = (RegionCoprocessorEnvironment) env;
        HRegion region = rce.getRegion();
				try{
						this.tableName = region.getTableDesc().getNameAsString();
						//make sure we only split around SI regions
            //noinspection ResultOfMethodCallIgnored
            Long.parseLong(tableName);

//						splitter = new BytesCopyTaskSplitter(region);
            splitter = NoOpTaskSplitter.INSTANCE; //don't split me!
				}catch(NumberFormatException nfe){
						splitter = NoOpTaskSplitter.INSTANCE;
				}
				runningTasks = SpliceDriver.driver().getTaskMonitor().registerRegion(region.getRegionInfo().getRegionNameAsString());
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
											 RpcCallback<SpliceMessage.SchedulerResponse> spliceSchedulerResponse) {

				try {
            byte[] start = spliceSchedulerRequest.getTaskStart().toByteArray();
            byte[] end = spliceSchedulerRequest.getTaskEnd().toByteArray();
            boolean allowSplits = spliceSchedulerRequest.getAllowSplits();
            validateSubmittability(start,end);

            RegionTask t = decodeTask(spliceSchedulerRequest);

            Collection<SizedInterval> splitPoints = split(t, start, end, allowSplits);
            TaskFutureContext[] contexts = submitAll(t,splitPoints,rce);
            SpliceMessage.SchedulerResponse.Builder responseBuilder = SpliceMessage.SchedulerResponse.newBuilder();
            SpliceMessage.TaskFutureResponse.Builder contextBuilder = SpliceMessage.TaskFutureResponse.newBuilder();
            for(TaskFutureContext context:contexts){
                contextBuilder.clear();
                SpliceMessage.TaskFutureResponse contextResponse = contextBuilder.setTaskNode(context.getTaskNode())
                        .setEstimatedCost(context.getEstimatedCost())
                        .setTaskId(ZeroCopyLiteralByteString.wrap(context.getTaskId()))
                        .setStartRow(ZeroCopyLiteralByteString.wrap(context.getStartRow())).build();
                responseBuilder = responseBuilder.addResponse(contextResponse);
            }

            spliceSchedulerResponse.run(responseBuilder.build());
				} catch (IOException e) {
						ResponseConverter.setControllerException(rpcController,e);
				}
		}

    private RegionTask decodeTask(SpliceSchedulerRequest spliceSchedulerRequest) {
        byte[] taskBytes = spliceSchedulerRequest.getClassBytes().toByteArray();
        Input input = new Input(taskBytes);
        Kryo kryo = SpliceKryoRegistry.getInstance().get();
        RegionTask t;
        try{
            t = (RegionTask)kryo.readClassAndObject(input);
        }finally{
            SpliceKryoRegistry.getInstance().returnInstance(kryo);
        }
        return t;
    }

    public TaskFutureContext[] submit(byte[] taskStart, byte[] taskEnd, final RegionTask task,boolean allowSplits) throws IOException {

        Collection<SizedInterval> splitPoints = split(task,taskStart,taskEnd,allowSplits);
				return submitAll(task, splitPoints, rce);
    }

    private void validateSubmittability(byte[] taskStart, byte[] taskEnd) throws IOException {
        //make sure that the task is fully contained within this region
        HRegion region = rce.getRegion();
        if(region.isClosed()|| region.isClosing())
            throw new NotServingRegionException("Region "+ region.getRegionNameAsString()+" is closing");
        if(!HRegionUtil.containsRange(region, taskStart, taskEnd))
            throw new IncorrectRegionException("Incorrect region for Task submission");
        if(!SpliceDriver.driver().isStarted())
            throw new ServerNotRunningYetException("Cannot submit tasks until Server is fully online. Please retry");
    }

    @SuppressWarnings("unchecked")
		private Collection<SizedInterval> split(RegionTask task, byte[] taskStart, byte[] taskEnd,boolean allowSplits) throws IOException {
				if(!allowSplits || !task.isSplittable())
						return Collections.singleton(new SizedInterval(taskStart, taskEnd, 0));

				if(LOG.isTraceEnabled())
						SpliceLogUtils.trace(LOG,"splitting task on table "+ this.tableName);
				return splitter.split(task,taskStart,taskEnd);
		}

		private TaskFutureContext[] submitAll(final RegionTask task,Collection<SizedInterval> splitPoints,RegionCoprocessorEnvironment rce) throws IOException{
				if(splitPoints.size()==1){
						//noinspection LoopStatementThatDoesntLoop
						for(SizedInterval split:splitPoints){
								return new TaskFutureContext[]{doSubmit(task,split.startKey,split.endKey,rce)};
						}
						return null; //can't happen
				}else{
            if(LOG.isTraceEnabled()){
                SpliceLogUtils.trace(LOG,"Creating %d splits",splitPoints.size());
            }
						TaskFutureContext[] all = new TaskFutureContext[splitPoints.size()];
						int i=0;
						for(SizedInterval split:splitPoints){
								all[i] = doSubmit(task.getClone(),split.startKey,split.endKey,rce);
								i++;
						}
						return all;
				}
		}

    private TaskFutureContext doSubmit(final RegionTask task,byte[] start, byte[] stop,RegionCoprocessorEnvironment rce) throws IOException {
        try {
            //prepare the task for this specific region
            task.prepareTask(start,stop,rce,ZkUtils.getZkManager());

            runningTasks.add(task);
						if(LOG.isTraceEnabled())
								SpliceLogUtils.trace(LOG,"Submitting task %s over range [%s,%s)",task.getTaskNode(),Bytes.toStringBinary(start),Bytes.toStringBinary(stop));
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
            return new TaskFutureContext(task.getTaskNode(),start,future.getTaskId(),future.getEstimatedCost());
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