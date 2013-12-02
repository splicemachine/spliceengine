package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.collect.Maps;
import com.splicemachine.derby.impl.job.coprocessor.RegionTask;
import com.splicemachine.job.TaskFuture;
import com.splicemachine.job.TaskScheduler;

import java.util.concurrent.*;

/**
 * @author Scott Fines
 * Date: 12/2/13
 */
public class TerracedPriorityTaskScheduler<T extends RegionTask> implements TaskScheduler<T> {
		private final TerracedThreadPool mainThreadPool;
		private final ThreadPoolExecutor overflowPool;

		private final ConcurrentMap<Integer,JobCapacity> terraceCapacityMap = Maps.newConcurrentMap();
		private final TerracedQueue<Runnable> terracedQueue;
		private JobCapacity overflowCapacity = new JobCapacity(1);

		public TerracedPriorityTaskScheduler(final int numThreads,final int numTerraces) {
				TerracedQueue.TerraceSizingStrategy sizingStrategy = new TerracedQueue.TerraceSizingStrategy() {
						@Override
						public int getNumTerraces() {
								return numTerraces+1; //include an overflow terrace
						}

						@Override
						public int getSize(int terrace) {
								/*
								 * We want a normalized distribution of terraces, where the number of threads
								 * available to each terrace is proportional to it's priority, but the sum of all
								 * threads available equals the total number of threads we provided
								 *
								 * The simplest way is to use (terrace+numThreads/numTerraces).
								 *
								 * We use numTerraces and not numTerraces+1, because the last terrace in the operation
								 * is the overflow terrace, which is treated differently
								 * TODO -sf- this isn't the only way to do this--make it more configurable
								 */
								if(terrace==numTerraces)
										return Integer.MAX_VALUE;
								return (numThreads+terrace)/numTerraces;
						}
				};
				BasicPlacementStrategy placementStrategy = new BasicPlacementStrategy(numTerraces,sizingStrategy) {
						@Override
						public int assignTerrace(Runnable item) {
								TaskCallable<T> callable = getCallable(item);
								int priority = callable.getPriority();
								if(priority>=numTerraces)
										priority = numTerraces-1;
								return priority;
						}

						@Override
						public boolean filter(Runnable item, int terrace) {
								TaskCallable<T> callable = getCallable(item);
								return terraceCapacityMap.get(callable.getPriority()).canRun(callable.getJobId());
						}
				};

				mainThreadPool = new TerracedThreadPool(numThreads,numThreads,60l,
								TimeUnit.SECONDS,placementStrategy,sizingStrategy,
								placementStrategy,new OverflowHandler());
				terracedQueue = (TerracedQueue<Runnable>) (mainThreadPool.getQueue());
				BlockingQueue<Runnable> overflowQueue = terracedQueue.terraceView(numTerraces);
				overflowPool = new ThreadPoolExecutor(0,Integer.MAX_VALUE,60l,TimeUnit.SECONDS, overflowQueue);
		}

		private TaskCallable<T> getCallable(Runnable item) {
				@SuppressWarnings("unchecked") CallableAccessibleFutureTask<Void> caft = (CallableAccessibleFutureTask<Void>)item;
				return (TaskCallable<T>)(caft.getCallable());
		}

		@Override
		public TaskFuture submit(T task) throws ExecutionException {
				//TODO -sf- add statistics
				ListeningTaskFuture<T> future = new ListeningTaskFuture<T>(task,terracedQueue.size(task.getPriority()));
				future.getTask().getTaskStatus().attachListener(future);
				mainThreadPool.submit(new TaskCallable<T>(task));
				return future;
		}

		private class OverflowHandler implements RejectedExecutionHandler{
				@Override
				public void rejectedExecution(Runnable r, ThreadPoolExecutor executor) {
						TaskCallable<T> callable = getCallable(r);

						TerracedQueue<Runnable> queue = (TerracedQueue<Runnable>)executor.getQueue();
						if(callable.getTask().getParentTaskId()!=null){
														/*
														 * It's a subtask, so push it to the overflow pool for execution
														 * IF it can be executed. With the overflow pool, this can only
														 * occur if nobody else is executing this job yet. Thus, we attempt
														 * to mark it started. If it can be started, then we submit it directly
														 * to the queue, knowing that there is at least one thread currently
														 * running in the thread pool which will look at the queue once completed
														 * (e.g. the thread currently running the other).
														 */
								String jobId = callable.getJobId();
								if(overflowCapacity.taskStarted(jobId))
										overflowPool.submit(callable);
								else
										overflowPool.getQueue().offer(r);
						}else{
								/*
								 * Forcibly push it to the correct position on the queue. We know that at least
								 * one thread must be executing currently, because the pool is full, so one of those
								 * will eventually notice the task, and execute it.
								 */
								queue.forceAdd(r);
						}
				}
		}
}
