package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.job.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import javax.management.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TaskScheduler that Steals Work from other Schedulers when it is empty.
 *
 * @author Scott Fines
 * Date: 12/8/13
 */
public class WorkStealingTaskScheduler<T extends Task> implements StealableTaskScheduler<T> {
		private static final Logger WORKER_LOG = Logger.getLogger(WorkStealingTaskScheduler.class);
		private final ThreadPoolExecutor executor;
    private final Stats stats = new Stats();
    private final JobMetrics jobMetrics = new JobMetrics();

		private final BlockingQueue<T> pendingTasks;
		private final List<Future<?>> workerFutures;
		private final List<StealableTaskScheduler<T>> otherSchedulers;
		private final long pollTimeout;

		private final AtomicInteger queueSize =  new AtomicInteger(0);

		public WorkStealingTaskScheduler(int numThreads,
																		 long pollTimeoutMs,
																		 Comparator<T> taskComparator,
																		 List<StealableTaskScheduler<T>> otherSchedulers,
																		 String schedulerName) {
				this.otherSchedulers = otherSchedulers;
				this.pollTimeout = pollTimeoutMs;

				if(taskComparator!=null)
						this.pendingTasks = new PriorityBlockingQueue<T>(numThreads,taskComparator);
				else
						this.pendingTasks = new LinkedBlockingQueue<T>();

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

				this.workerFutures =  Collections.synchronizedList(new ArrayList<Future<?>>());
				for(int i=0;i<numThreads;i++){
						workerFutures.add(executor.submit(new Worker()));
				}
		}

		@Override
		public TaskFuture submit(T task) throws ExecutionException {
				int waitTime = doSubmit(task);
				return new ListeningTaskFuture<T>(task,waitTime);
		}

		private int doSubmit(T task) {
				stats.submittedCount.incrementAndGet();
				int waitTime = queueSize.getAndIncrement();
				pendingTasks.offer(task);
				return waitTime;
		}

		@Override
		public TaskFuture tryExecute(T task) throws ExecutionException {
				if(WORKER_LOG.isTraceEnabled())
						WORKER_LOG.trace("Attempting to submit task "+ Bytes.toString(task.getTaskId()));
				/*
				 * We can only accept this task if there are idle workers.
				 * An idle worker corresponds to an empty queue, so check
				 * the queue size. However, as the getSize-checkSize-submit
				 * operation is not atomic, we cannot rely on that as a
				 * safety valve. Instead, we keep an atomic counter in sync.
				 */
				boolean success;
				do{
						int currentQueueSize = queueSize.get();
						int maxWorkers = executor.getMaximumPoolSize();
						if(currentQueueSize>=maxWorkers){
								stats.executeFailureCount.incrementAndGet();
								return null; //queue isn't empty, so can't execute
						}
						success = queueSize.compareAndSet(currentQueueSize,currentQueueSize+1);
				}while(!success);

				stats.submittedCount.incrementAndGet();
				pendingTasks.offer(task);
            TaskFuture taskFuture = new ListeningTaskFuture<T>(task,0);
            jobMetrics.updateTask(taskFuture.getTaskId(), task.getJobId(), taskFuture.getStatus().name());
            return taskFuture;
		}

		@Override
		public void resubmit(T task) {
				doSubmit(task);
		}

		@Override
		public T steal() {
				T last = pendingTasks.poll();
				if(last!=null)
						queueSize.decrementAndGet();
				return last;
		}

		public void shutdown(){
				Iterator<Future<?>> workerIter = workerFutures.iterator();
				while(workerIter.hasNext()){
						Future<?> future = workerIter.next();
						future.cancel(true);
						workerIter.remove();
				}
				executor.shutdownNow();
		}

		public void setNumWorkers(int newNumWorkers){
				executor.setCorePoolSize(newNumWorkers);
				executor.setMaximumPoolSize(newNumWorkers);

				while(workerFutures.size()>newNumWorkers){
						//TODO -sf- ensure that running tasks are resubmitted without cancellation
						//or failure
						workerFutures.remove(0).cancel(true);
				}

				while(workerFutures.size()<newNumWorkers){
						workerFutures.add(executor.submit(new Worker()));
				}
		}

		public StealableTaskSchedulerManagement getManagement(){
				return stats;
		}

		public void registerJMX(MBeanServer mbs, String baseJmx) throws MalformedObjectNameException, NotCompliantMBeanException, InstanceAlreadyExistsException, MBeanRegistrationException {
				ObjectName name = new ObjectName(baseJmx+":type=StealableTaskSchedulerManagement");
				mbs.registerMBean(stats,name);
		}

		private class Stats implements TaskStatus.StatusListener,
						StealableTaskSchedulerManagement{
				private final AtomicInteger numPending = new AtomicInteger(0);
				private final AtomicInteger numExecuting = new AtomicInteger(0);
				private final AtomicLong failedCount = new AtomicLong(0l);
				private final AtomicLong submittedCount = new AtomicLong(0l);
				private final AtomicLong invalidatedCount = new AtomicLong(0l);
				private final AtomicLong cancelledCount = new AtomicLong(0l);
				private final AtomicLong successCount = new AtomicLong(0l);

				private final AtomicLong stolenCount = new AtomicLong(0l);
				private final AtomicLong executeFailureCount = new AtomicLong(0l);

				@Override public int getCurrentWorkers() { return executor.getActiveCount(); }
				@Override public void setCurrentWorkers(int maxWorkers) { setNumWorkers(maxWorkers); }
				@Override public long getTotalCompletedTasks() { return successCount.get(); }
				@Override public long getTotalFailedTasks() { return failedCount.get(); }
				@Override public long getTotalCancelledTasks() { return cancelledCount.get(); }
				@Override public long getTotalInvalidatedTasks() { return invalidatedCount.get(); }
				@Override public int getNumExecutingTasks() { return numExecuting.get(); }
				@Override public int getNumPendingTasks() { return numPending.get(); }
				@Override public long getTotalStolenTasks() { return stolenCount.get(); }
				@Override public long getTotalSubmitFailures() { return executeFailureCount.get(); }
				@Override public long getTotalSubmittedTasks() { return submittedCount.get(); }

				@Override
				public void statusChanged(Status oldStatus,
																	Status newStatus,
																	TaskStatus taskStatus) {
						if(oldStatus!=null){
								switch (oldStatus) {
										case PENDING:
												numPending.decrementAndGet();
												break;
										case EXECUTING:
												numExecuting.decrementAndGet();
												break;
								}
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
								T next = pendingTasks.poll();
								if(next!=null){
										try{
												execute(next,WorkStealingTaskScheduler.this);
												continue interruptLoop;
										}finally{
												queueSize.decrementAndGet();
										}
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
										next = pendingTasks.poll(pollTimeout,TimeUnit.MILLISECONDS);
										if(next!=null){
												try{
														execute(next,WorkStealingTaskScheduler.this);
												}finally{
														queueSize.decrementAndGet();
												}
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
						if(WORKER_LOG.isDebugEnabled())
								WORKER_LOG.debug("Executing task "+ Bytes.toString(next.getTaskId()));
						next.getTaskStatus().attachListener(stats);
						try {
								new TaskCallable<T>(next).call();
						}catch(InterruptedException ie){
								if(WORKER_LOG.isInfoEnabled())
										WORKER_LOG.info("Interrupted during execution of task "+ Bytes.toShort(next.getTaskId()));
								//told to shut down--resubmit back to the used scheduler's queue
								usedScheduler.resubmit(next);
						}catch (Exception e) {
								WORKER_LOG.error("Unexepcted exception calling task "+ Bytes.toString(next.getTaskId()),e);
						}finally{
								next.getTaskStatus().detachListener(stats);
                            jobMetrics.updateTask(next.getTaskId(), next.getJobId(),next.getTaskStatus().getStatus().name());
						}
				}
		}
}
