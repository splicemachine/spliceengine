package com.splicemachine.job;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * A Representation of a Running Job.
 *
 * In general, A Job can be in one of the following states:
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

    /**
     * Cancel the job and any outstanding tasks yet to be completed.
     *
     * Once this method is called, any pending tasks will not be executed; tasks which are currently
     * being executed may or may not be cancelled during execution, at the discretion of the TaskScheduler
     * (although one hopes that tasks will be cancelled if possible).
     *
     * @throws ExecutionException if something goes wrong
     */
    void cancel() throws ExecutionException;

    /**
     * Gets the estimated cost to run the entire job.
     *
     * @return the estimated cost to run the entire job.
     * @throws ExecutionException
     */
    double getEstimatedCost() throws ExecutionException;

    /**
     * Gets statistics related to this job. Statistics are only guaranteed to be non-null upon
     * <em>successful completion</em> of the task.
     * @return statistics for this job, or {@code null} if statistics are not available.
     */
    JobStats getJobStats();

    void cleanup() throws ExecutionException;

    int getNumTasks();

    int getRemainingTasks();
}
