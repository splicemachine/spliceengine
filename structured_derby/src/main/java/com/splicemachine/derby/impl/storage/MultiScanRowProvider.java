package com.splicemachine.derby.impl.storage;

import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationProtocol;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.stats.RegionStats;
import com.splicemachine.derby.stats.SinkStats;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.JobFuture;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
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
    @Override
    public void shuffleRows( SpliceObserverInstructions instructions,
                            RegionStats stats) throws StandardException {
        List<Scan> scans = getScans();
        HTableInterface table = SpliceAccessManager.getHTable(getTableName());
        LinkedList<JobFuture> jobs = Lists.newLinkedList();
        LinkedList<JobFuture> completedJobs = Lists.newLinkedList();
        try{
            for(Scan scan:scans){
                jobs.add(doShuffle(table, instructions, stats, scan));
            }

            //we have to wait for all of them to complete, so just wait in order
            Throwable error = null;
            try{
                while(jobs.size()>0){
                    JobFuture next = jobs.pop();
                    next.completeAll();
                    completedJobs.add(next);
                }
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
                        SpliceDriver.driver().getJobScheduler().cleanupJob(completedJob);
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
                SpliceDriver.driver().getJobScheduler().cleanupJob(jobToCancel);
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
    protected abstract List<Scan> getScans() throws StandardException;


/********************************************************************************************************************/
    /*private helper methods*/
    private JobFuture doShuffle(HTableInterface table,
                           SpliceObserverInstructions instructions,
                           RegionStats stats, Scan scan) throws StandardException {
        SpliceUtils.setInstructions(scan, instructions);
        OperationJob job = new OperationJob(scan,instructions,table);
        try {
            return SpliceDriver.driver().getJobScheduler().submit(job);
        } catch (Throwable throwable) {
            throw Exceptions.parseException(throwable);
        }
    }

    private static class Callback implements Batch.Callback<SinkStats>{
        private final RegionStats stats;

        private Callback(RegionStats stats) {
            this.stats = stats;
        }

        @Override
        public void update(byte[] region, byte[] row, SinkStats result) {
            this.stats.addRegionStats(region,result);
        }
    }

    private static class Call implements Batch.Call<SpliceOperationProtocol,SinkStats>{
        private final Scan scan;
        private final SpliceObserverInstructions instructions;

        private Call(Scan scan, SpliceObserverInstructions instructions) {
            this.scan = scan;
            this.instructions = instructions;
        }

        @Override
        public SinkStats call(SpliceOperationProtocol instance) throws IOException {
            try {
                return instance.run(scan,instructions);
            } catch (StandardException e) {
                throw new IOException(e);
            }
        }
    }
}
