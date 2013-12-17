package com.splicemachine.derby.impl.job.scheduler;

import com.splicemachine.job.Task;
import com.splicemachine.job.TaskFuture;
import com.splicemachine.job.TaskScheduler;

import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Date: 12/4/13
 */
public interface StealableTaskScheduler<T extends Task> extends TaskScheduler<T> {

		/**
		 * Submits the task only if there are idle workers available to immediately
		 * execute this task.
		 *
		 * <p>This differs from {@link #submit(com.splicemachine.job.Task)}</p> in that
		 * this method will return {@code null} if there are no threads immediately available
		 * to take the work.
		 *
		 * @param task the task to be attempted.
		 * @return a TaskFuture if successfully submitted, or {@code null} if
		 * unable to sumit.
		 * @throws ExecutionException
		 */
		TaskFuture tryExecute(T task) throws ExecutionException;

		void resubmit(T task);

		T steal();
}
