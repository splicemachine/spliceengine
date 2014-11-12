package com.splicemachine.derby.impl.job.scheduler;

import com.splicemachine.job.JobFuture;
import com.splicemachine.job.JobResults;
import com.splicemachine.job.JobStats;
import com.splicemachine.job.Status;

import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * JobFuture wrapper for a JobResults instance. Useful when needing to make a synchronously
 * executed job conform to the asynchronous API.
 *
 * @author P Trolard
 *         Date: 17/01/2014
 */
public class JobFutureFromResults implements JobFuture {
    private final JobResults results;

    public JobFutureFromResults(JobResults results){
        this.results = results;
    }

    @Override
    public Status getStatus() throws ExecutionException {
        return Status.COMPLETED;
    }

    @Override
    public void completeAll(StatusHook statusHook)
            throws ExecutionException, InterruptedException, CancellationException {
        // no-op
    }

    @Override
    public void completeNext(StatusHook hook)
            throws ExecutionException, InterruptedException, CancellationException {
        // no-op
    }

    @Override
    public void cancel() throws ExecutionException {
        // no-op
    }

    @Override
    public double getEstimatedCost() throws ExecutionException {
        return 0;
    }

    @Override
    public JobStats getJobStats() {
        return results.getJobStats();
    }

    @Override
    public void cleanup() throws ExecutionException {
        // no-op
    }

    @Override
    public void addCleanupTask(Callable<Void> closable) {
        // no-op
    }

		@Override
		public void intermediateCleanup() throws ExecutionException {
				//no-op
		}

		@Override
		public void addIntermediateCleanupTask(Callable<Void> callable) {
			//no-op
		}

		@Override
    public int getNumTasks() {
        return results.getJobStats().getNumTasks();
    }

    @Override
    public int getRemainingTasks() {
        return 0;
    }

    @Override
    public byte[][] getAllTaskIds() {
        return new byte[results.getJobStats().getTaskStats().size()][];
    }
}
