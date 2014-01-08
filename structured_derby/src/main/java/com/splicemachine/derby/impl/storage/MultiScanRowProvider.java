package com.splicemachine.derby.impl.storage;

import com.google.common.collect.Lists;
import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.hbase.SpliceObserverInstructions;
import com.splicemachine.derby.hbase.SpliceOperationRegionObserver;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.iapi.storage.RowProvider;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.store.access.SpliceAccessManager;
import com.splicemachine.derby.utils.Exceptions;
import com.splicemachine.derby.utils.SpliceUtils;
import com.splicemachine.job.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.derby.iapi.error.StandardException;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Collection;
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
    protected SpliceRuntimeContext spliceRuntimeContext;
    
    private boolean shuffled = false;
		private List<JobFuture> jobFutures;

    @Override
    public JobResults shuffleRows( SpliceObserverInstructions instructions) throws StandardException {
        shuffled = true;
        List<Scan> scans = getScans();
        instructions.setSpliceRuntimeContext(spliceRuntimeContext);
        HTableInterface table = SpliceAccessManager.getHTable(getTableName());
				LinkedList<Pair<JobFuture,JobInfo>> outstandingJobs = Lists.newLinkedList();
        jobFutures =  Lists.newArrayList();
        List<JobStats> stats = Lists.newArrayList();
        try{
            long start = System.nanoTime();
            for(Scan scan:scans){
								long startTimeMs = System.currentTimeMillis();
								OperationJob job = getJob(table,instructions,scan);
								JobFuture jobFuture = doShuffle(job);
								JobInfo info = new JobInfo(job.getJobId(),jobFuture.getNumTasks(),startTimeMs);
								info.tasksRunning(jobFuture.getAllTaskIds());
								instructions.getSpliceRuntimeContext().getStatementInfo().addRunningJob(info);
								outstandingJobs.add(Pair.newPair(jobFuture, info));
								jobFutures.add(jobFuture);
            }

            //we have to wait for all of them to complete, so just wait in order
            Throwable error = null;
            try{
                while(outstandingJobs.size()>0){
                    Pair<JobFuture,JobInfo> next = outstandingJobs.pop();
										JobFuture jobFuture = next.getFirst();
										JobInfo jobInfo = next.getSecond();

										try{
												jobFuture.completeAll(jobInfo);
										}catch(ExecutionException e){
												jobInfo.failJob();
												throw e;
										}
										instructions.getSpliceRuntimeContext().getStatementInfo().completeJob(jobInfo);
                    jobFutures.add(jobFuture);
                    stats.add(jobFuture.getJobStats());
                }

                long stop = System.nanoTime();
                //construct the job stats to return
                return new CompositeJobResults(jobFutures,stats,stop-start);
            } catch (InterruptedException e) {
                error = e;
                throw Exceptions.parseException(e);
            } catch (ExecutionException e) {
                error = e.getCause();
                throw Exceptions.parseException(e.getCause());
            }finally{
                if(error!=null){
                    cancelAll(outstandingJobs);
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

		private void cancelAll(Collection<Pair<JobFuture,JobInfo>> jobs) {
				//cancel all remaining tasks
				for(Pair<JobFuture,JobInfo> jobToCancel:jobs){
						try {
								jobToCancel.getFirst().cancel();
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
			if(jobFutures!=null){
						for(JobFuture completedJob:jobFutures){
								try {
										completedJob.cleanup();
								} catch (ExecutionException e) {
										SpliceLogUtils.error(LOG,"Unable to clean up job",e.getCause());
								}
						}
			}
		}

    /********************************************************************************************************************/
    /*private helper methods*/
    private JobFuture doShuffle(OperationJob job) throws StandardException {
        try {
            return SpliceDriver.driver().getJobScheduler().submit(job);
        } catch (Throwable throwable) {
            throw Exceptions.parseException(throwable);
        }
    }

		private OperationJob getJob(HTableInterface table, SpliceObserverInstructions instructions, Scan scan) {
				if(scan.getAttribute(SpliceOperationRegionObserver.SPLICE_OBSERVER_INSTRUCTIONS)==null)
						SpliceUtils.setInstructions(scan, instructions);
				boolean readOnly = !(instructions.getTopOperation() instanceof DMLWriteOperation);
				return new OperationJob(scan,instructions,table,readOnly);
		}


}
