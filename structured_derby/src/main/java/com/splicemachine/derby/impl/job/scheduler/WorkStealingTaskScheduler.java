package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.job.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.util.Comparator;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Scott Fines
 * Date: 12/8/13
 */
public class WorkStealingTaskScheduler<T extends Task> implements StealableTaskScheduler<T> {
		private static final Logger WORKER_LOG = Logger.getLogger(WorkStealingTaskScheduler.class);
		private final ThreadPoolExecutor executor;
		private final Stats stats = new Stats();

		private final BlockingQueue<T> outstandingTasks;
		private final BlockingQueue<Future<?>> workerFutures;
		private final List<StealableTaskScheduler<T>> otherSchedulers;
		private final long pollTimeout;

		private final AtomicInteger queueSize =  new AtomicInteger(0);

		public WorkStealingTaskScheduler(int numThreads,
																		 long pollTimeoutMs,
																		 List<StealableTaskScheduler<T>> otherSchedulers,
																		 String schedulerName) {
				this.otherSchedulers = otherSchedulers;
				this.pollTimeout = pollTimeoutMs;

				Comparator<T> comparator = new Comparator<T>() {
						@Override
						public int compare(T o1, T o2) {
								return o1.getPriority() - o2.getPriority();
						}
				};
				this.outstandingTasks = new PriorityBlockingQueue<T>(numThreads,comparator);
				ThreadFactory factory = new ThreadFactoryBuilder()
								.setNameFormat(schedulerName+"-thread-%d")
								.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
										@Override
										public void uncaughtException(Thread t, Throwable e) {
												WORKER_LOG.error("Uncaught error on thread "+ t.getName(),e);
										}
								}).build();
				this.executor = new ThreadPoolExecutor(numThreads,numThreads,
								60l,TimeUnit.MILLISECONDS,new LinkedBlockingQueue<Runnable>(),factory);
				executor.allowCoreThreadTimeOut(true);

				this.workerFutures =  new ArrayBlockingQueue<Future<?>>(numThreads);
				for(int i=0;i<numThreads;i++){
						workerFutures.add(executor.submit(new Worker()));
				}
		}

		@Override
		public TaskFuture submit(T task) throws ExecutionException {
				stats.numPending.incrementAndGet();
				int waitTime = queueSize.getAndIncrement();
				outstandingTasks.offer(task);
				return new ListeningTaskFuture<T>(task,waitTime);
		}

		@Override
		public TaskFuture tryExecute(T task) throws ExecutionException {
				/*
				 * We can only accept this task if there are idle workers.
				 * An idle worker corresponds to an empty queue, so check
				 * the queue size. However, as the getSize-checkSize-submit
				 * operation is not atomic, we cannot rely on that as a
				 * safety valve. Instead, we keep an atomic counter in sync.
				 */
				if(!queueSize.compareAndSet(0,1)){
						stats.executeFailureCount.incrementAndGet();
						return null; //queue isn't empty, so can't execute
				}

				return submit(task);
		}

		@Override
		public void resubmit(T task) {
				stats.numPending.incrementAndGet();
				queueSize.getAndIncrement();
				outstandingTasks.offer(task);
		}

		@Override
		public T steal() {
				T last = outstandingTasks.poll();
				if(last!=null)
						queueSize.decrementAndGet();
				return last;
		}

		public void shutdown(){
				Future<?> future;
				while((future = workerFutures.poll())!=null){
						future.cancel(true);
				}
				executor.shutdownNow();
		}

		public void setNumWorkers(int newNumWorkers){
				executor.setMaximumPoolSize(newNumWorkers);
				executor.setCorePoolSize(newNumWorkers);

				while(workerFutures.size()>newNumWorkers){
						//TODO -sf- ensure that running tasks are resubmitted without cancellation
						//or failure
						workerFutures.poll().cancel(true);
				}

				while(workerFutures.size()<newNumWorkers){
						workerFutures.add(executor.submit(new Worker()));
				}
				stats.numWorkers = newNumWorkers;
		}

		public StealableTaskSchedulerManagement getManagement(){
				return stats;
		}


		private class Stats implements TaskStatus.StatusListener,
						StealableTaskSchedulerManagement{
				private final AtomicInteger numPending = new AtomicInteger(0);
				private final AtomicInteger numExecuting = new AtomicInteger(0);
				private final AtomicLong failedCount = new AtomicLong(0l);
				private final AtomicLong invalidatedCount = new AtomicLong(0l);
				private final AtomicLong cancelledCount = new AtomicLong(0l);
				private final AtomicLong successCount = new AtomicLong(0l);

				private final AtomicLong stolenCount = new AtomicLong(0l);
				private final AtomicLong executeFailureCount = new AtomicLong(0l);

				private volatile int numWorkers;

				@Override public int getCurrentWorkers() { return numWorkers; }
				@Override public void setCurrentWorkers(int maxWorkers) { setNumWorkers(maxWorkers); }
				@Override public long getTotalCompletedTasks() { return successCount.get(); }
				@Override public long getTotalFailedTasks() { return failedCount.get(); }
				@Override public long getTotalCancelledTasks() { return cancelledCount.get(); }
				@Override public long getTotalInvalidatedTasks() { return invalidatedCount.get(); }
				@Override public int getNumExecutingTasks() { return numExecuting.get(); }
				@Override public int getNumPendingTasks() { return numPending.get(); }
				@Override public long getTotalStolenTasks() { return stolenCount.get(); }
				@Override public long getTotalSubmitFailures() { return executeFailureCount.get(); }

				@Override
				public void statusChanged(Status oldStatus,
																	Status newStatus,
																	TaskStatus taskStatus) {
						switch (oldStatus) {
								case PENDING:
										numPending.decrementAndGet();
										break;
								case EXECUTING:
										numExecuting.decrementAndGet();
										break;
						}
						switch (newStatus) {
								case INVALID:
										invalidatedCount.incrementAndGet();
										break;
								case PENDING:
										numPending.incrementAndGet();
										break;
								case EXECUTING:
										numExecuting.incrementAndGet();
										break;
								case FAILED:
										failedCount.incrementAndGet();
										break;
								case COMPLETED:
										successCount.incrementAndGet();
										break;
								case CANCELLED:
										cancelledCount.incrementAndGet();
										break;
						}
				}
		}

		private class Worker implements Runnable {
				@Override
				public void run() {
						interruptLoop:
						while(!Thread.currentThread().isInterrupted()){
								T next = outstandingTasks.poll();
								if(next!=null){
										queueSize.decrementAndGet();
										execute(next,WorkStealingTaskScheduler.this);
										continue;
								}
								if(WORKER_LOG.isTraceEnabled())
										WORKER_LOG.trace("No work found, attempting to steal from other schedulers");

								/*
								 * There are no tasks available on my queue, attempt
								 * to steal from other schedulers IN LIST ORDER
								 */
								for(StealableTaskScheduler<T> scheduler:otherSchedulers){
										next = scheduler.steal();
										if(next!=null){
												stats.stolenCount.incrementAndGet();
												execute(next,WorkStealingTaskScheduler.this);
												continue interruptLoop;
										}
								}

								/*
								 * There are no tasks available on other queues, so
								 * do a blocking wait on our own for a while
								 */

								if(WORKER_LOG.isTraceEnabled())
										WORKER_LOG.trace("No tasks available to steal, waiting on our queue");
								try{
										next = outstandingTasks.poll(pollTimeout,TimeUnit.MILLISECONDS);
										if(next!=null){
												queueSize.decrementAndGet();
												execute(next,WorkStealingTaskScheduler.this);
										}
								} catch (InterruptedException e) {
										/*
										 * We were told to shut down. Mark the current thread
										 * as interrupted to ensure that we break
										 * from the loop
										 */
										Thread.currentThread().interrupt();
								}
						}
				}

				private void execute(T next, StealableTaskScheduler<T> usedScheduler) {
						next.getTaskStatus().attachListener(stats);
						try {
								new TaskCallable<T>(next).call();
						}catch(InterruptedException ie){
								//told to shut down--resubmit back to the used scheduler's queue
								usedScheduler.resubmit(next);
						}catch (Exception e) {
								WORKER_LOG.error("Unexepcted exception calling task "+ Bytes.toString(next.getTaskId()),e);
						}finally{
								next.getTaskStatus().detachListener(stats);
						}
				}
		}
}
