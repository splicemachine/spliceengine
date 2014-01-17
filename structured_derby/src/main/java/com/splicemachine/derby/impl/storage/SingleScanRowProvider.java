package com.splicemachine.derby.impl.storage;

import com.google.common.base.Function;
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
import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.job.operation.SinkTask;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.OperationSink;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.*;
import com.splicemachine.si.impl.TransactionId;
import com.splicemachine.utils.SpliceLogUtils;
import com.splicemachine.utils.SpliceZooKeeperManager;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;
import org.jruby.RubyProcess;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CancellationException;
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
        shuffled = true;
        Scan scan = toScan();
        instructions.setSpliceRuntimeContext(spliceRuntimeContext);
        spliceRuntimeContext.setCurrentTaskId(SpliceDriver.driver().getUUIDGenerator().nextUUIDBytes());
        if (scan == null) {
            //operate locally
            SpliceOperation op = instructions.getTopOperation();
            op.init(SpliceOperationContext.newContext(op.getActivation()));
            try {
                OperationSink opSink = OperationSink.create((SinkingOperation) op, null, instructions.getTransactionId());

                JobStats stats;
                if (op instanceof DMLWriteOperation)
                    stats = new LocalTaskJobStats(opSink.sink(((DMLWriteOperation) op).getDestinationTable(), spliceRuntimeContext));
                else {
                    byte[] tempTableBytes = SpliceDriver.driver().getTempTable().getTempTableName();
                    stats = new LocalTaskJobStats(opSink.sink(tempTableBytes, spliceRuntimeContext));
                }

                return new SimpleJobResults(stats, null);
            } catch (Exception e) {
                throw Exceptions.parseException(e);
            }
        } else {
            return finishShuffle(asyncShuffleRows(instructions, scan));
        }
    }

     @Override
     public List<Pair<JobFuture,JobInfo>> asyncShuffleRows(SpliceObserverInstructions instructions) throws StandardException {
        return asyncShuffleRows(instructions, toScan());
    }

    private List<Pair<JobFuture,JobInfo>> asyncShuffleRows(SpliceObserverInstructions instructions, Scan scan) throws StandardException {
        return Collections.singletonList( doAsyncShuffle(instructions, scan) );
    }

    @Override
    public JobResults finishShuffle(List<Pair<JobFuture,JobInfo>> jobs) throws StandardException{

        StandardException baseError = null;

        List<JobStats> stats = new LinkedList<JobStats>();
        JobResults results = null;

        long start = System.nanoTime();

        for (Pair<JobFuture, JobInfo> job : jobs) {
            try {
                job.getFirst().completeAll(job.getSecond());

                stats.add(job.getFirst().getJobStats());
                LOG.error(String.format("Async job completed for %s", Bytes.toString(getTableName())));

            } catch (ExecutionException ee) {
                SpliceLogUtils.error(LOG, ee);
                if (job.getSecond() != null) {
                    job.getSecond().failJob();
                }
                baseError = Exceptions.parseException(ee.getCause());
                throw baseError;
            } catch (InterruptedException e) {
                SpliceLogUtils.error(LOG, e);
                if (job.getSecond() != null) {
                    job.getSecond().failJob();
                }
                baseError = Exceptions.parseException(e);
                throw baseError;
            } finally {
                if (jobFuture != null) {
                    try {
                        job.getFirst().cleanup();
                        //jobFuture.cleanup();
                    } catch (ExecutionException e) {
                        if (baseError == null)
                            baseError = Exceptions.parseException(e.getCause());
                    }
                }
                if (baseError != null) {
                    SpliceLogUtils.logAndThrow(LOG, baseError);
                }
            }
        }

        long end = System.nanoTime();

        if (jobs.size() > 1) {
            results = new CompositeJobResults(Lists.transform(jobs, new Function<Pair<JobFuture, JobInfo>, JobFuture>() {
                @Override
                public JobFuture apply(Pair<JobFuture, JobInfo> job) {
                    return job.getFirst();
                }
            }), stats, end - start);
        } else if (jobs.size() == 1) {
            results = new SimpleJobResults(stats.iterator().next(), jobs.get(0).getFirst());
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

        if (jobFuture != null) {
            try {
                jobFuture.cleanup();
            } catch (ExecutionException e) {
                throw Exceptions.parseException(e);
            }
        }
    }

    public Pair<JobFuture,JobInfo> doAsyncShuffle(SpliceObserverInstructions instructions, Scan scan) throws StandardException {

        // PJT We're getting scan == null here
        if (scan.getAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS) == null)
            SpliceUtils.setInstructions(scan, instructions);

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

            LOG.error(String.format("Async job submitted for %s (job %s)", Bytes.toString(getTableName()), job.getJobId()));

            //future.addCleanupTask(table);

            return Pair.newPair(jobFuture, jobInfo);

        } catch (ExecutionException ee) {
            SpliceLogUtils.error(LOG, ee);
            if (jobInfo != null)
                jobInfo.failJob();
            baseError = Exceptions.parseException(ee.getCause());
            throw baseError;
        } finally {
            if (jobFuture != null && baseError != null) {
                /*
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
                */
                if (table != null) {
                    try {
                        table.close();
                    } catch (Exception e) {
                        SpliceLogUtils.error(LOG, "Error closing HTable instance", e);
                    }
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
