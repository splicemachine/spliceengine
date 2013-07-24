package com.splicemachine.derby.impl.storage;

import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationRegionObserver;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.OperationSink;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobStats;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
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
    private boolean shuffled = false;

    @Override
    public JobStats shuffleRows(SpliceObserverInstructions instructions) throws StandardException {
        shuffled=true;
        Scan scan = toScan();

        if(scan==null){
            //operate locally
            SpliceOperation op = instructions.getTopOperation();
            op.init(SpliceOperationContext.newContext(op.getActivation()));
            try{
                OperationSink opSink = OperationSink.create((SinkingOperation) op,null);
                if(op instanceof DMLWriteOperation)
                    return new LocalTaskJobStats(opSink.sink(((DMLWriteOperation)op).getDestinationTable()));
                else
                    return new LocalTaskJobStats(opSink.sink(SpliceConstants.TEMP_TABLE_BYTES));
            } catch (Exception e) {
                throw Exceptions.parseException(e);
            }
        }else{

            List<JobFuture> jobFutures = asyncShuffleRows(instructions, scan);
            return finishShuffle(jobFutures);

        }
    }

     @Override
     public List<JobFuture> asyncShuffleRows(SpliceObserverInstructions instructions) throws StandardException {

        return asyncShuffleRows(instructions, toScan());

    }

    private List<JobFuture> asyncShuffleRows(SpliceObserverInstructions instructions, Scan scan) throws StandardException {

        return Collections.singletonList( doAsyncShuffle(instructions, scan) );
    }

    @Override
    public JobStats finishShuffle(List<JobFuture> jobFutures) throws StandardException{

        StandardException baseError = null;

        List<JobStats> stats = new LinkedList();
        JobStats results = null;

        long start = System.nanoTime();

        for(JobFuture jobFuture : jobFutures){
            try {
                jobFuture.completeAll();

                stats.add(jobFuture.getJobStats());

            } catch (ExecutionException ee) {
                SpliceLogUtils.error(LOG, ee);
                baseError = Exceptions.parseException(ee.getCause());
                throw baseError;
            } catch (InterruptedException e) {
                SpliceLogUtils.error(LOG, e);
                baseError = Exceptions.parseException(e);
                throw baseError;
            }finally{
                if(jobFuture!=null){
                    try{
                        jobFuture.cleanup();
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

        long end = System.nanoTime();

        if(jobFutures.size() > 1){
            results = new CompositeJobStats(stats, end-start);
        }else if(jobFutures.size() == 1){
            results = stats.iterator().next();
        }

        return results;
    }


    /**
     * @return a scan representation of the row provider, or {@code null} if the operation
     * is to be shuffled locally.
     */
    public abstract Scan toScan();

    @Override
    public void close() {
        if(!shuffled) return; //no need to clean temp, it's just a scan
        Scan scan = toScan();
        try {
            SpliceDriver.driver().getTempCleaner().deleteRange(SpliceUtils.getUniqueKey(),scan.getStartRow(),scan.getStopRow());
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }

    public JobFuture doAsyncShuffle(SpliceObserverInstructions instructions, Scan scan) throws StandardException {

        if(scan.getAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS)==null)
            SpliceUtils.setInstructions(scan,instructions);

        //get transactional stuff from scan

        //determine if top operation writes data

        boolean readOnly = !(instructions.getTopOperation() instanceof DMLWriteOperation);
        HTableInterface table = SpliceAccessManager.getHTable(getTableName());

        OperationJob job = new OperationJob(scan,instructions,table,readOnly);
        JobFuture future = null;
        StandardException baseError = null;

        try {

            future = SpliceDriver.driver().getJobScheduler().submit(job);
            future.addCleanupTask(table);

            return future;

        } catch (ExecutionException ee) {
            SpliceLogUtils.error(LOG, ee);
            baseError = Exceptions.parseException(ee.getCause());
            throw baseError;
        }finally{
            if(future!=null && baseError != null){
                try{
                    future.cleanup();

                    if(table != null){
                        try{
                            table.close();
                        }catch(Exception e) {
                            SpliceLogUtils.error(LOG, "Error closing HTable instance", e);
                        }
                    }

                } catch (ExecutionException e) {
                    SpliceLogUtils.error(LOG, "Error cleaning up JobFuture", e);
                }
            }
            if(baseError!=null){
                SpliceLogUtils.logAndThrow(LOG,baseError);
            }
        }
    }



    /********************************************************************************************************************/
    /*private helper methods*/

    private class LocalTaskJobStats implements JobStats {
        private final TaskStats stats;

        public LocalTaskJobStats(TaskStats stats) {
            this.stats = stats;
        }

        @Override public int getNumTasks() { return 1; }
        @Override public int getNumSubmittedTasks() { return 1; }
        @Override public int getNumCompletedTasks() { return 1; }
        @Override public int getNumFailedTasks() { return 0; }
        @Override public int getNumInvalidatedTasks() { return 0; }
        @Override public int getNumCancelledTasks() { return 0; }
        @Override public long getTotalTime() { return stats.getTotalTime(); }
        @Override public String getJobName() { return "localJob"; }

        @Override
        public List<String> getFailedTasks() {
            return Collections.emptyList();
        }

        @Override
        public Map<String, TaskStats> getTaskStats() {
            return Collections.singletonMap("localTask",stats);
        }
    }
}
