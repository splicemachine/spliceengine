package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.job.Task;
import com.splicemachine.job.TaskFuture;
import com.splicemachine.job.TaskScheduler;

import java.util.concurrent.*;

/**
 * Unbounded, fully concurrent task scheduler.
 *
 * @author Scott Fines
 * Date: 12/4/13
 */
public class ExpandingTaskScheduler<T extends Task> implements StealableTaskScheduler<T> {
		private final ExecutorService executor;

		public ExpandingTaskScheduler() {
				ThreadFactory factory = new ThreadFactoryBuilder().setNameFormat("expandingWorker-thread-%d").build();
				this.executor = new ThreadPoolExecutor(0,
								Integer.MAX_VALUE,60l, TimeUnit.MILLISECONDS,new SynchronousQueue<Runnable>(),factory);
		}

		@Override
		public TaskFuture submit(T task) throws ExecutionException {
				executor.submit(new TaskCallable<T>(task));
				return new ListeningTaskFuture<T>(task,0);
		}

		@Override
		public boolean isShutdown() {
				return executor.isShutdown();
		}

		@Override public TaskFuture tryExecute(T task) throws ExecutionException { return submit(task); }
		@Override
		public void resubmit(T task)  {
				try {
						submit(task);
				} catch (ExecutionException e) {
						throw new RuntimeException(e); //should never happen
				}
		}
		@Override public T steal() { return null; }
}
