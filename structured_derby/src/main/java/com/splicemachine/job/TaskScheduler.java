package com.splicemachine.job;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;

/**
 * @author Scott Fines
 *         Created on: 4/3/13
 */
public interface TaskScheduler<T extends Task> {

		public static interface RejectionHandler<T extends Task>{
				public void rejected(T task);
		}

    TaskFuture submit(T task) throws ExecutionException;

		boolean isShutdown();

		public static class ExceptionRejectionHandler<T extends Task> implements RejectionHandler<T>{
				private static final ExceptionRejectionHandler INSTANCE = new ExceptionRejectionHandler();
				private ExceptionRejectionHandler() { }
				@SuppressWarnings("unchecked")
				public static <T extends Task> ExceptionRejectionHandler<T> instance(){
						return (ExceptionRejectionHandler<T>)INSTANCE;
				}
				@Override public void rejected(T task) { throw new RejectedExecutionException(); }
		}
}
