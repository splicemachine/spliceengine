package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationProtocol;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.RegionStats;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobStats;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Abstract RowProvider which assumes a single Scan entity covers the entire data range.
 *
 * @author Scott Fines
 * Created on: 3/26/13
 */
public abstract class SingleScanRowProvider  implements RowProvider {

    private static final Logger LOG = Logger.getLogger(SingleScanRowProvider.class);

    @Override
    public JobStats shuffleRows(SpliceObserverInstructions instructions ) throws StandardException {
        Scan scan = toScan();
        if(scan==null){
            //operate locally
            SpliceOperation op = instructions.getTopOperation();
            op.init(SpliceOperationContext.newContext(op.getActivation()));
            try{
                return new LocalTaskJobStats(op.sink());
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
        }else{
            HTableInterface table = SpliceAccessManager.getHTable(getTableName());
            return doShuffle(table, instructions, scan);
        }
    }


    /**
     * @return a scan representation of the row provider, or {@code null} if the operation
     * is to be shuffled locally.
     */
    protected abstract Scan toScan();

/********************************************************************************************************************/
    /*private helper methods*/

    private JobStats doShuffle(HTableInterface table,
                           SpliceObserverInstructions instructions, Scan scan) throws StandardException {
        //TODO -sf- attach statistics
        SpliceUtils.setInstructions(scan,instructions);
        OperationJob job = new OperationJob(scan,instructions,table);
        JobFuture future = null;
        StandardException baseError = null;
        try {
            future = SpliceDriver.driver().getJobScheduler().submit(job);
            //wait for everyone to complete, or somebody to error out
            future.completeAll();

            return future.getJobStats();
        } catch (ExecutionException ee) {
            baseError = Exceptions.parseException(ee.getCause());
            throw baseError;
        } catch (InterruptedException e) {
            baseError = Exceptions.parseException(e);
            throw baseError;
        }finally{
            if(future!=null){
                try{
                    SpliceDriver.driver().getJobScheduler().cleanupJob(future);
                } catch (ExecutionException e) {
                    if(baseError==null)
                        baseError = Exceptions.parseException(e.getCause());
                }
            }
            if(baseError!=null){
                SpliceLogUtils.logAndThrow(LOG,baseError);
            }
        }
    }

    /*
     * Convenience wrapper to update statistics
     */
    private static class Callback implements Batch.Callback<TaskStats>{
        private final RegionStats stats;

        private Callback(RegionStats stats) {
            this.stats = stats;
        }

        @Override
        public void update(byte[] region, byte[] row, TaskStats result) {
            this.stats.addRegionStats(region,result);
        }
    }

    /*
     * Convenience wrapper around the execution phase
     */
    private static class Call implements Batch.Call<SpliceOperationProtocol,TaskStats>{
        private final Scan scan;
        private final SpliceObserverInstructions instructions;

        private Call(Scan scan, SpliceObserverInstructions instructions) {
            this.scan = scan;
            this.instructions = instructions;
        }

        @Override
        public TaskStats call(SpliceOperationProtocol instance) throws IOException {
            try {
                return instance.run(scan,instructions);
            } catch (StandardException e) {
                throw new IOException(e);
            }
        }
    }

    private class LocalTaskJobStats implements JobStats {
        private final TaskStats stats;

        public LocalTaskJobStats(TaskStats stats) {
            this.stats = stats;
        }

        @Override
        public int getNumTasks() {
            return 1;
        }

        @Override
        public int getNumSubmittedTasks() {
            return 1;
        }

        @Override
        public int getNumCompletedTasks() {
            return 1;
        }

        @Override
        public int getNumFailedTasks() {
            return 0;
        }

        @Override
        public int getNumInvalidatedTasks() {
            return 0;
        }

        @Override
        public int getNumCancelledTasks() {
            return 0;
        }

        @Override
        public long getTotalTime() {
            return stats.getTotalTime();
        }

        @Override
        public String getJobName() {
            return "localJob";
        }

        @Override
        public Map<String, TaskStats> getTaskStats() {
            return Collections.singletonMap("localTask",stats);
        }
    }
}
