package com.splicemachine.derby.impl.storage;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationRegionObserver;
import com.splicemachine.derby.iapi.sql.execute.SinkingOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperationContext;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.OperationSink;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.jruby.RubyProcess;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
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

	protected SpliceRuntimeContext spliceRuntimeContext;
    private static final Logger LOG = Logger.getLogger(SingleScanRowProvider.class);
    private boolean shuffled = false;
		private JobFuture jobFuture;

    @Override
    public JobResults shuffleRows(SpliceObserverInstructions instructions) throws StandardException {
    	shuffled=true;
        Scan scan = toScan();
        instructions.setSpliceRuntimeContext(spliceRuntimeContext);
				spliceRuntimeContext.setCurrentTaskId(SpliceDriver.driver().getUUIDGenerator().nextUUIDBytes());
        if(scan==null){
            //operate locally
            SpliceOperation op = instructions.getTopOperation();
            op.init(SpliceOperationContext.newContext(op.getActivation()));
            try{
                OperationSink opSink = OperationSink.create((SinkingOperation) op,null,instructions.getTransactionId());

								JobStats stats;
                if(op instanceof DMLWriteOperation)
                    stats =new LocalTaskJobStats(opSink.sink(((DMLWriteOperation)op).getDestinationTable(), spliceRuntimeContext));
                else{
										byte[] tempTableBytes = SpliceDriver.driver().getTempTable().getTempTableName();
                    stats =new LocalTaskJobStats(opSink.sink(tempTableBytes, spliceRuntimeContext));
								}

								return new SimpleJobResults(stats,null);
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
    public JobResults finishShuffle(List<JobFuture> jobFutures) throws StandardException{

        StandardException baseError = null;

        List<JobStats> stats = new LinkedList();
        JobResults results = null;

        long start = System.nanoTime();

        for(JobFuture jobFuture : jobFutures){
            try {
                // PJT add real JobInfo
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
            results = new CompositeJobResults(jobFutures, stats, end - start);
        }else if(jobFutures.size() == 1){
            results = new SimpleJobResults(stats.iterator().next(), jobFutures.get(0));
        }
         return results;

    }

	@Override
	public SpliceRuntimeContext getSpliceRuntimeContext() {
		return spliceRuntimeContext;
	}

    /**
     * @return a scan representation of the row provider, or {@code null} if the operation
     * is to be shuffled locally.
     */
    public abstract Scan toScan();

		@Override
		public void close() throws StandardException {

				if(jobFuture!=null){
						try{
								jobFuture.cleanup();
						}catch(ExecutionException e){
								throw Exceptions.parseException(e);
						}
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
        StandardException baseError = null;
        JobInfo jobInfo = null;

        try {

            long start = System.currentTimeMillis();
            jobFuture = SpliceDriver.driver().getJobScheduler().submit(job);
            jobInfo = new JobInfo(job.getJobId(), jobFuture.getNumTasks(), start);
            jobInfo.setJobFuture(jobFuture);
            byte[][] allTaskIds = jobFuture.getAllTaskIds();
            for (byte[] tId : allTaskIds) {
                jobInfo.taskRunning(tId);
            }
            instructions.getSpliceRuntimeContext().getStatementInfo().addRunningJob(jobInfo);

            //future.addCleanupTask(table);

            return jobFuture;

        } catch (ExecutionException ee) {
            SpliceLogUtils.error(LOG, ee);
            if (jobInfo != null)
                jobInfo.failJob();
            baseError = Exceptions.parseException(ee.getCause());
            throw baseError;
        } finally {
            if (jobFuture != null && baseError != null) {
                try {
                    jobFuture.cleanup();

                    if (table != null) {
                        try {
                            table.close();
                        } catch (Exception e) {
                            SpliceLogUtils.error(LOG, "Error closing HTable instance", e);
                        }
                    }

                } catch (ExecutionException e) {
                    SpliceLogUtils.error(LOG, "Error cleaning up JobFuture", e);
                }
            }
            if (baseError != null) {
                SpliceLogUtils.logAndThrow(LOG, baseError);
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
        public List<byte[]> getFailedTasks() {
            return Collections.emptyList();
        }

        @Override
        public List<TaskStats> getTaskStats() {
            return Arrays.asList(stats);
        }
    }
}
