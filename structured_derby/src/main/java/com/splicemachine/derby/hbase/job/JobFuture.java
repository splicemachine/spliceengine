package com.splicemachine.derby.hbase.job;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * A Job can be in one of 5 states:
 *
 * Pending: All Component tasks within the job are awaiting execution--none have started
 * Executing: At least one component task within the job is currently executing
 * Failed: At least one component task within the job has failed in an irrecoverable way.
 * Complete: All Component tasks have completed successfully
 * Cancelled: The cancel() method has been called.
 *
 * @author Scott Fines
 * Created on: 4/3/13
 */
public interface JobFuture {

    /**
     * Get the current status of the job.
     *
     * @return the current status of the job.
     *
     * @throws ExecutionException
     */
    Status getStatus() throws ExecutionException;

    /**
     * Waits if necessary for the Computation to complete.
     *
     * @throws ExecutionException wrapping any underlying exception which is thrown.
     */
    void completeAll() throws ExecutionException,InterruptedException,CancellationException;

    /**
     * Waits if necessary for the next task in the computation to complete. Will not wait
     * until all computations are complete, but only until the next task has been completed. If
     * a Task has already completed, then this will return immediately.
     *
     * @throws ExecutionException
     */
    void completeNext() throws ExecutionException, InterruptedException,CancellationException;

    void cancel() throws ExecutionException;

    double getEstimatedCost() throws ExecutionException;

    int getNumTasks() throws ExecutionException;

    int getNumCompletedTasks() throws ExecutionException;

    int getNumFailedTasks() throws ExecutionException;

    /**
     * When a Job is cancelled, some tasks within that job may have already completed or failed. This number
     * is the number of jobs that were cancelled <em>before</em> completing or failing.
     *
     * @return
     * @throws ExecutionException
     */
    int getNumCancelledTasks() throws ExecutionException;
}
