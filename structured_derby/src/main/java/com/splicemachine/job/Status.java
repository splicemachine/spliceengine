package com.splicemachine.job;

/**
 * @author Scott Fines
 * Created on: 4/3/13
 */
public enum  Status {
    /**
     * Marked when a Task or Job has been invalidated by the engine, and must be resubmitted by the client.
     *
     * Generally, an Invalidated Task/Job is one that has been submitted, but the state of the executor has
     * changed out from under the Task/Job, and intervention by the submitter is necessary. For example, in a
     * Coprocessor-based Task Executor, the HBase Region may have split before the task has been executed, making
     * a task against that region no longer viable. Thus, these tasks are marked invalid, and the Job executor
     * must resubmit those tasks under the new region world.
     */
    INVALID,
    /**
     * Marked when the task or Job is waiting to be executed.
     */
    PENDING,
    /**
     * Marked when a task is currently being executed.
     */
    EXECUTING,
    /**
     * Marked when a job or Task has failed in an irreconcilable way. Failed tasks or jobs should <em>never</em>
     * be resubmitted. Jobs or Tasks which can be resubmitted should be set to the {@link #INVALID} state instead.
     */
    FAILED,
    /**
     * Marked when a task has completed successfully.
     */
    COMPLETED,
    /**
     * Marked when a job has been cancelled.
     */
    CANCELLED
}
