package com.splicemachine.derby.impl.job.scheduler;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.coprocessor.SpliceSchedulerProtocol;
import com.splicemachine.derby.impl.job.coprocessor.TaskFutureContext;
import com.splicemachine.hbase.table.BoundCall;
import com.splicemachine.utils.SpliceZooKeeperManager;
import com.splicemachine.utils.kryo.KryoPool;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import java.io.IOException;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Created on: 9/17/13
 */
public class JobControl extends BaseJobControl {

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
            table.coprocessorExec(SpliceSchedulerProtocol.class, start, stop,
                    new BoundCall<SpliceSchedulerProtocol, byte[]>() {
                        @Override
                        public byte[] call(SpliceSchedulerProtocol instance) throws IOException {
                            throw new UnsupportedOperationException();
                        }

                        @Override
                        public byte[] call(byte[] startKey, byte[] stopKey, SpliceSchedulerProtocol instance) throws IOException {
                            KryoPool kp =SpliceKryoRegistry.getInstance();
                            Kryo kryo = kp.get();
                            try{
                                Output op = new Output(128,-1);
                                kryo.writeClassAndObject(op,task);

                                return instance.submit(startKey,stopKey,op.getBuffer(),tryCount==0); //only allow splitting the first time
                            }finally{
                                kp.returnInstance(kryo);
                            }
                        }
                    }, new Batch.Callback<byte[]>() {
                        @Override
                        public void update(byte[] region, byte[] row, byte[] resultBytes) {
                            KryoPool kp =SpliceKryoRegistry.getInstance();
                            Kryo kryo = kp.get();
                            try{
                                Input input = new Input(resultBytes);
                                int size = input.readInt();
                                for(int i=0;i<size;i++){
                                    TaskFutureContext result = kryo.readObject(input,TaskFutureContext.class);
                                    RegionTaskControl control = new RegionTaskControl(result.getStartRow(), task, JobControl.this, result, tryCount, zkManager);
                                    tasksToWatch.add(control);
                                    taskChanged(control);
                                }

                            }finally{
                                kp.returnInstance(kryo);
                            }
                        }
                    }
            );
        }catch (Throwable throwable) {
            throw new ExecutionException(throwable);
        }
    }
}
