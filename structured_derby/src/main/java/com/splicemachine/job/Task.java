package com.splicemachine.job;

import com.splicemachine.si.api.Txn;

import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Created on: 4/3/13
 */
public interface Task {

    void markStarted() throws ExecutionException, CancellationException;

    void markCompleted() throws ExecutionException;

    void markFailed(Throwable error) throws ExecutionException;

    void markCancelled() throws ExecutionException;

    void markCancelled(boolean propagate) throws ExecutionException;

    void execute() throws ExecutionException,InterruptedException;

    boolean isCancelled() throws ExecutionException;

    byte[] getTaskId();

    TaskStatus getTaskStatus();

    void markInvalid() throws ExecutionException;

    boolean isInvalidated();

    void cleanup() throws ExecutionException;

    int getPriority();

    /**
     * @return the transaction that this task is operating under, or {@code null} if
		 * the task is non-transactional
     */
		Txn getTxn();

		/**
		 * @return the parent task id (if this is a child task), or {@code null}
		 * if this has no parent task (e.g. if it is not a subtask)
		 */
		byte[] getParentTaskId();

		String getJobId();
}
