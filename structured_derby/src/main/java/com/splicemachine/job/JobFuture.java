package com.splicemachine.job;

import java.io.Closeable;
import java.util.concurrent.Callable;
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
    void completeAll(StatusHook statusHook) throws ExecutionException,InterruptedException,CancellationException;

    /**
     * Waits if necessary for the next task in the computation to complete. Will not wait
     * until all computations are complete, but only until the next task has been completed. If
     * a Task has already completed, then this will return immediately.
     *
     * @throws ExecutionException
     */
    void completeNext(StatusHook hook) throws ExecutionException, InterruptedException,CancellationException;

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

		/**
		 * Cleans up any temporary data held by the Job during its execution. Temporary data
		 * includes (but is not limited to):
		 *
		 * <ul>
		 *     <li>Data held in Temporary space</li>
		 *     <li>StateManagement data held in a State Machine (e.g. ZooKeeper, etc)</li>
		 * </ul>
		 *
		 * As a result, one should be careful to <em>only</em> call then when the results of the
		 * job are <em>no longer required</em>. Otherwise, it is possible that incorrect results may
		 * occur!
		 *
		 * @throws ExecutionException If something goes wrong during cleanup
		 */
    void cleanup() throws ExecutionException;

		/**
		 * Cleans up any intermediate state data held by the Job, but <em>not</em> temporary data.
		 *
		 * This method should be called whenever the job is completed, to clean up any state which
		 * does not need to be retained for a long period of time.
		 *
		 * @throws ExecutionException
		 */
		void intermediateCleanup() throws ExecutionException;

		/**
		 * Add an arbitrary task to be accomplished when the Job is cleaned up.
		 *
		 * @param closable
		 */
    void addCleanupTask(Callable<Void> closable);

		void addIntermediateCleanupTask(Callable<Void> callable);

    int getNumTasks();

    int getRemainingTasks();

    byte[][] getAllTaskIds();

    public interface StatusHook {
        void success(byte[] taskId);

        void failure(byte[] taskId);

        void cancelled(byte[] taskId);

        void invalidated(byte[] taskId);
    }
}
