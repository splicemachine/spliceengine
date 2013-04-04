package com.splicemachine.job;

import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public interface JobScheduler<J extends Job> {

    JobFuture submit(J job) throws ExecutionException;

    void cleanupJob(JobFuture future) throws ExecutionException;
}
