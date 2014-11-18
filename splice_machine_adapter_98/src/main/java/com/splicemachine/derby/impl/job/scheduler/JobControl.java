package com.splicemachine.derby.impl.job.scheduler;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.google.protobuf.ByteString;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.coprocessor.TaskFutureContext;
import com.splicemachine.hbase.table.BoundCall;
import com.splicemachine.hbase.table.SpliceRpcController;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceZooKeeperManager;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.ipc.BlockingRpcCallback;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Created on: 9/17/13
 */
public class JobControl extends BaseJobControl {
    private static final Logger LOG = Logger.getLogger(JobControl.class);

    public JobControl(CoprocessorJob job, String jobPath,SpliceZooKeeperManager zkManager, int maxResubmissionAttempts, JobMetrics jobMetrics){
    	super(job,jobPath,zkManager,maxResubmissionAttempts,jobMetrics);
    }
    /*
     * Physically submits a Task.
     */
    public void submit(final RegionTask task,
            Pair<byte[], byte[]> range,
            HTableInterface table,
            final int tryCount) throws ExecutionException {
	final byte[] start = range.getFirst();
	final byte[] stop = range.getSecond();

	try{
		table.coprocessorService(SpliceMessage.SpliceSchedulerService.class,start,stop,
		        new BoundCall<SpliceMessage.SpliceSchedulerService, TaskFutureContext[]>() {
		            @Override
		            public TaskFutureContext[] call(SpliceMessage.SpliceSchedulerService instance) throws IOException {
		                throw new UnsupportedOperationException();
		            }
		
		            @Override
		            public TaskFutureContext[] call(byte[] startKey, byte[] stopKey, SpliceMessage.SpliceSchedulerService instance) throws IOException {
		                SpliceMessage.SpliceSchedulerRequest request = encodeTask(startKey, stopKey, task,tryCount);
													BlockingRpcCallback<SpliceMessage.SchedulerResponse> rpcCallback = new BlockingRpcCallback<SpliceMessage.SchedulerResponse>();
													SpliceRpcController controller = new SpliceRpcController();
													instance.submit(controller,request,rpcCallback);
		
		                Throwable error = controller.getThrowable();
		                if(error!=null) throw Exceptions.getIOException(error);
		
		                SpliceMessage.SchedulerResponse spliceSchedulerResponse = rpcCallback.get();
		                int numContexts = spliceSchedulerResponse.getResponseCount();
		                TaskFutureContext[] contexts = new TaskFutureContext[numContexts];
		                for(int i=0;i<numContexts;i++){
		                    SpliceMessage.TaskFutureResponse response = spliceSchedulerResponse.getResponse(i);
		                    TaskFutureContext context = new TaskFutureContext(response.getTaskNode(),
		                            response.getStartRow().toByteArray(),
		                            response.getTaskId().toByteArray(),
		                            response.getEstimatedCost());
		                    contexts[i] = context;
		                }
		                return contexts;
		            }
		        }, new Batch.Callback<TaskFutureContext[]>() {
		            @Override
		            public void update(byte[] region, byte[] row, TaskFutureContext[] results) {
		                for (TaskFutureContext result : results) {
		                    RegionTaskControl control = new RegionTaskControl(result.getStartRow(), task, JobControl.this, result, tryCount, zkManager);
		                    tasksToWatch.add(control);
		                    taskChanged(control);
		                }
		            }
		        });
		}catch (Throwable throwable) {
			throw new ExecutionException(throwable);
		}
    }

    private static SpliceMessage.SpliceSchedulerRequest encodeTask(byte[] startKey, byte[] stopKey, RegionTask task,int tryCount) {
		SpliceMessage.SpliceSchedulerRequest.Builder requestBuilder = SpliceMessage.SpliceSchedulerRequest.newBuilder()
		    .setTaskStart(ByteString.copyFrom(startKey))
		    .setAllowSplits(tryCount==0) //only allow splits on the first submission attempt
		    .setTaskEnd(ByteString.copyFrom(stopKey));
		
		Output out = new Output(1024,-1);
		KryoPool kryoPool = SpliceKryoRegistry.getInstance();
		Kryo kryo = kryoPool.get();
		try{
			kryo.writeClassAndObject(out, task);
		}finally{
			kryoPool.returnInstance(kryo);
		}
		out.flush();
		return requestBuilder.setClassBytes(ByteString.copyFrom(out.toBytes())).build();
		}
	}