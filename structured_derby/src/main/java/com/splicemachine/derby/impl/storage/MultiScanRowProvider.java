package com.splicemachine.derby.impl.storage;

import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationProtocol;
import com.splicemachine.derby.hbase.SpliceOperationRegionObserver;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.RegionStats;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.hbase.TempCleaner;
import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobStats;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

/**
 * Abstract RowProvider which assumes that multiple scans are required to
 * cover the entire row space.
 *
 * @author Scott Fines
 * Created on: 3/26/13
 */
public abstract class MultiScanRowProvider implements RowProvider {
    private static final Logger LOG = Logger.getLogger(MultiScanRowProvider.class);

    private boolean shuffled = false;
    @Override
    public JobStats shuffleRows( SpliceObserverInstructions instructions) throws StandardException {
        shuffled = true;
        List<Scan> scans = getScans();
        HTableInterface table = SpliceAccessManager.getHTable(getTableName());
        LinkedList<JobFuture> jobs = Lists.newLinkedList();
        LinkedList<JobFuture> completedJobs = Lists.newLinkedList();
        List<JobStats> stats = Lists.newArrayList();
        try{
            long start = System.nanoTime();
            for(Scan scan:scans){
                jobs.add(doShuffle(table, instructions, scan));
            }

            //we have to wait for all of them to complete, so just wait in order
            Throwable error = null;
            try{
                while(jobs.size()>0){
                    JobFuture next = jobs.pop();
                    next.completeAll();
                    completedJobs.add(next);
                    stats.add(next.getJobStats());
                }

                long stop = System.nanoTime();
                //construct the job stats to return
                return new CompositeJobStats(stats,stop-start);
            } catch (InterruptedException e) {
                error = e;
                throw Exceptions.parseException(e);
            } catch (ExecutionException e) {
                error = e.getCause();
                throw Exceptions.parseException(e.getCause());
            }finally{
                if(error!=null){
                    cancelAll(jobs);
                }
                for(JobFuture completedJob:completedJobs){
                    try {
                        completedJob.cleanup();
                    } catch (ExecutionException e) {
                        SpliceLogUtils.error(LOG,"Unable to clean up job",e.getCause());
                    }
                }
            }

        }finally{
            try {
                table.close();
            } catch (IOException e) {
                SpliceLogUtils.logAndThrow(Logger.getLogger(MultiScanRowProvider.class),
                        Exceptions.parseException(e));
            }
        }
    }

    private void cancelAll(LinkedList<JobFuture> jobs) {
        //cancel all remaining tasks
        for(JobFuture jobToCancel:jobs){
            try {
                jobToCancel.cancel();
                jobToCancel.cleanup();
            } catch (ExecutionException e) {
                SpliceLogUtils.error(LOG, "Unable to cancel job", e.getCause());
            }
        }
    }

    /**
     * Get all disjoint scans which cover the row space.
     *
     * @return all scans which cover the row space
     * @throws StandardException if something goes wrong while getting scans.
     */
    public abstract List<Scan> getScans() throws StandardException;


    @Override
    public void close() {
        if(!shuffled) //no-op if scans aren't performed
        try {
            List<Scan> scans = getScans();
            TempCleaner cleaner = SpliceDriver.driver().getTempCleaner();
            for(Scan scan:scans){
                cleaner.deleteRange(SpliceUtils.getUniqueKey(),scan.getStartRow(),scan.getStopRow());
            }
        } catch (StandardException e) {
            throw new RuntimeException(e);
        }
    }

    /********************************************************************************************************************/
    /*private helper methods*/
    private JobFuture doShuffle(HTableInterface table,
                           SpliceObserverInstructions instructions,
                           Scan scan) throws StandardException {
        if(scan.getAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS)==null)
            SpliceUtils.setInstructions(scan, instructions);
        boolean readOnly = !(instructions.getTopOperation() instanceof DMLWriteOperation);
        OperationJob job = new OperationJob(scan,instructions,table,readOnly);
        try {
            return SpliceDriver.driver().getJobScheduler().submit(job);
        } catch (Throwable throwable) {
            throw Exceptions.parseException(throwable);
        }
    }

    private class CompositeJobStats implements JobStats {
        private final List<JobStats> stats;
        private final long totalTime;

        public CompositeJobStats(List<JobStats> stats, long totalTime) {
            this.stats= stats;
            this.totalTime = totalTime;
        }

        @Override
        public int getNumTasks() {
            int numTasks=0;
            for(JobStats stat:stats){
                numTasks+=stat.getNumTasks();
            }
            return numTasks;
        }

        @Override
        public long getTotalTime() {
            return totalTime;
        }

        @Override
        public int getNumSubmittedTasks() {
            int numTasks=0;
            for(JobStats stat:stats){
                numTasks+=stat.getNumSubmittedTasks();
            }
            return numTasks;
        }

        @Override
        public int getNumCompletedTasks() {
            int numTasks=0;
            for(JobStats stat:stats){
                numTasks+=stat.getNumCompletedTasks();
            }
            return numTasks;
        }

        @Override
        public int getNumFailedTasks() {
            int numTasks=0;
            for(JobStats stat:stats){
                numTasks+=stat.getNumFailedTasks();
            }
            return numTasks;
        }

        @Override
        public int getNumInvalidatedTasks() {
            int numTasks=0;
            for(JobStats stat:stats){
                numTasks+=stat.getNumInvalidatedTasks();
            }
            return numTasks;
        }

        @Override
        public int getNumCancelledTasks() {
            int numTasks=0;
            for(JobStats stat:stats){
                numTasks+=stat.getNumCancelledTasks();
            }
            return numTasks;
        }

        @Override
        public Map<String, TaskStats> getTaskStats() {
            Map<String,TaskStats> allTaskStats = new HashMap<String,TaskStats>();
            for(JobStats stat:stats){
                allTaskStats.putAll(stat.getTaskStats());
            }
            return allTaskStats;
        }

        @Override
        public String getJobName() {
            return "multiScanJob"; //TODO -sf- use a better name here
        }

        @Override
        public List<byte[]> getFailedTasks() {
            List<byte[]> failedTasks = Lists.newArrayList();
            for(JobStats stat:stats){
                failedTasks.addAll(stat.getFailedTasks());
            }
            return failedTasks;
        }
    }
}
