package com.splicemachine.job;

import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public interface JobScheduler<J extends Job> {

    JobFuture submit(J job) throws ExecutionException;

    JobFuture submit(J job, JobStatusLogger jobStatusLogger) throws ExecutionException;

    JobSchedulerManagement getJobMetrics();

		long[] getActiveOperations() throws ExecutionException;
}
