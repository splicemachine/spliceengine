package com.splicemachine.concurrent;

import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author Scott Fines
 *         Date: 4/11/14
 */
public class SameThreadExecutorService implements ExecutorService{
		private static final ExecutorService INSTANCE = new SameThreadExecutorService();

		public static ExecutorService instance(){
				return INSTANCE;
		}

		private SameThreadExecutorService(){}

		@Override public void shutdown() {  }
		@Override public List<Runnable> shutdownNow() { return null; }
		@Override public boolean isShutdown() { return false; }
		@Override public boolean isTerminated() { return false; }
		@Override public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException { return false; }

		@Override
		public <T> Future<T> submit(Callable<T> task) {
				try {
						T call = task.call();
						return value(call);
				} catch (Exception e) {
					return error(e);
				}
		}

		private <T> Future<T> error(Exception e) {
				return new CompletedFuture<T>(null,e);
		}

		private <T> Future<T> value(T call) {
				return new CompletedFuture<T>(call,null);
		}

		@Override
		public <T> Future<T> submit(Runnable task, T result) {
				task.run();
				return value(result);
		}

		@Override
		public Future<?> submit(Runnable task) {
				task.run();
				return value(null);
		}

		@Override
		public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks) throws InterruptedException {
				List<Future<T>> completedFutures = Lists.newArrayListWithExpectedSize(tasks.size());
				for(Callable<T> task:tasks){
						completedFutures.add(submit(task));
				}
				return completedFutures;
		}

		@Override
		public <T> List<Future<T>> invokeAll(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException {
				return invokeAll(tasks);
		}

		@Override
		public <T> T invokeAny(Collection<? extends Callable<T>> tasks) throws InterruptedException, ExecutionException {
				for(Callable<T> task:tasks){
						if(task!=null)
								return submit(task).get();
				}
				throw new IllegalArgumentException("No tasks to execute!");
		}

		@Override
		public <T> T invokeAny(Collection<? extends Callable<T>> tasks, long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
				return invokeAny(tasks);
		}

		@Override
		public void execute(Runnable command) {
				command.run();
		}

		private static class CompletedFuture<T> implements Future<T>{
				private final T element;
				private final Throwable error;

				private CompletedFuture(T element, Throwable error) {
						this.element = element;
						this.error = error;
				}

				@Override public boolean cancel(boolean mayInterruptIfRunning) { return false; }

				@Override public boolean isCancelled() { return false; }

				@Override public boolean isDone() { return true; }

				@Override
				public T get() throws InterruptedException, ExecutionException {
						if(error!=null) throw new ExecutionException(error);
						return element;
				}

				@Override
				public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
						return get();
				}
		}
}
