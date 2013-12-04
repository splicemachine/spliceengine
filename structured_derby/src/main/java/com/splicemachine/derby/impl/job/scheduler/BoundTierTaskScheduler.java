package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.base.Preconditions;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.job.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import java.lang.reflect.Array;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A Bounded, Tiered, cooperative Task Scheduler.
 *
 * <p>The basic structure is an ordered sequence of Tiers, where a Tier
 * is associated to a specific, fixed priority pool. Within a tier is a
 * pool of threads and a queue of tasks to be run, with each thread inside of a tier
 * taking tasks off the queue for execution (similar in some sense to a hierarchical
 * actor model).
 *
 * <h3>Tiers</h3>
 *
 * <p>Tiers have three essential elements: a priority level, a fixed pool of workers,
 * and an execution queue. Making use of it are two essential mechanisms: a "soft" submission,
 * and a forced submission. A "soft" submission will only succeed if there is an idle worker
 * on that tier which is able to immediately begin execution, while a forced submission will
 * push the task onto a queue for execution whenever a worker can get to it. A forced submission
 * does not guarantee immediate execution by a thread, while a "soft" submission does.
 *
 * <p>As a terminology note, a Tier is said to be <em>saturated</em> when all workers are occupied. When a
 * tier is saturated, soft submission attempts will fail, and force submissions will not execute immediately.
 *
 * <h3>Task submission</h3>
 *
 * <p>A task has an assigned priority, which maps directly to a Tier. When submitted, that task
 * will first be soft-submitted to the Tier for its priority. If the tier is not saturated, it will
 * accept the task and this scheduler's job will be finished.
 *
 * <p>However, If the tier is saturated, the scheduler will attempt to "shrug" the task onto a lower-priority
 * tier. To do this, the scheduler will sequentially (in ascending order) search tiers with a lower priority until
 * either one is found which is not saturated or no tiers are available.
 *
 * <p>If a tier with a lower priority is found which is not saturated, the task will be submitted for immediate
 * execution on that tier instead of the correct priority tier.
 *
 * <p>If all tiers with equals or lesser priority are saturated, then the task is considered to have "overflowed",
 * and a decision must be made as to what to do. There are three possibilities (corresponding to the
 * {@link OverflowHandler.OverflowPolicy} enumeration), {@code ENQUEUE}, {@code REJECT}, and {@code OVERFLOW}. The
 * exact mechanism for deciding which policy to apply is left to configuration (via the {@link OverflowHandler}
 * abstraction).
 *
 * <p>When the overflow policy is {@code REJECT}, then the task is rejected, and the configured
 * RejectionHandler is engaged to reject the task.
 *
 * <p>When the overflow policy is {@code ENQUEUE}, then the task is force-submitted to the tier with the proper
 * priority level. That is, the tier which has the highest priority less than or equal to the task's priority
 * is forced to enqueue the task for later execution.
 *
 * <h3>Task Overflow</h3>
 *
 * <p>There is a special tier which is separate from all others, called the <em>Overflow Tier</em>. This
 * is a functionally-unbounded thread pool which executes tasks that have overflowed but cannot be rejected
 * or enqueued.
 *
 * <p>When the overflow policy is {@code OVERFLOW}, then the task is forcibly submitted to the Overflow Tier.
 *
 * <h3>WorkStealing and Idle Workers</h3>
 *
 * <p>It is always possible that a worker for a given tier becomes idle and has no additional work to do. In this
 * case, it is allowed to "steal" tasks which have been enqueued at higher priorities. To do this, it first attempts
 * to steal work from the Overflow Tier. If the overflow tier contains no tasks, then it will sequentially search
 * (in descending order) for tasks on higher-priority tiers which are waiting for resources. If one is found, the
 * worker will execute that task.
 *
 * <h2>Worker Allocation</h2>
 * There are three ways in which a worker can be told to execute a task:
 *
 * 1.The task has the proper priority for that worker
 * 2. The task has a higher priority than the worker's tier, but the correct tier was saturated at submission time,
 * while this worker was idle.
 * 3. The worker completed all work for his tier, and stole a task from a higher-priority tier for execution.
 *
 * <p>In practice, this has several consequences.
 *
 * <p>First, priority inversions are not possible. High priority threads can never steal work from lower priority
 * tiers, so even if a lower priority tier is saturated, those tasks will not be executed by a higher priority pool.
 *
 * <p>Second, this allows more tasks of a fixed priority to be executed than just is present on its tier directly. This
 * favors a configuration with relatively few ultra-high-priority threads and a lot of medium and medium-low priority
 * threads, as the high-priority tasks can always take medium priority threads, but medium priority tasks will not
 * be able to take high-priority threads.
 *
 * @author Scott Fines
 * Date: 12/3/13
 */
public class BoundTierTaskScheduler<T extends Task> implements TaskScheduler<T> {

		private final int numPriorityLevels;
		private final Tier[] tiers;
		private final OverflowHandler overflowHandler;
		private final StealableTaskScheduler<T> overflowScheduler;

		/**
		 * Strategy for handling situations when the scheduler
		 * is fully saturated and this task cannot be executed
		 * in a normal means.
		 */
		public static interface OverflowHandler{
				public static enum OverflowPolicy{
						/**
						 * used to indicate that the task should
						 * be forcibly enqueued at it's assigned level
						 */
						ENQUEUE,
						/**
						 * Used to indicate that the task should be
						 * rejected, and the RejectionExecutionHandler should
						 * be invoked
						 */
						REJECT,
						/**
						 * Used to indicate that the task should be pushed
						 * to the Overflow pool for execution
						 */
						OVERFLOW
				}
				public OverflowPolicy shouldOverflow(Task t);
		}


		public final RejectionHandler<T> rejectionHandler;


		public BoundTierTaskScheduler(int[] threadCounts,
																	OverflowHandler overflowHandler,
																	StealableTaskScheduler<T> overflowScheduler){
				this(threadCounts, overflowHandler,overflowScheduler,TaskScheduler.ExceptionRejectionHandler.<T>instance());
		}

 		public BoundTierTaskScheduler(int[] threadCounts,
																	OverflowHandler overflowHandler,
																	StealableTaskScheduler<T> overflowScheduler,
																	RejectionHandler<T> rejectionHandler) {
				this.overflowHandler = overflowHandler;
				this.rejectionHandler = rejectionHandler;
				this.numPriorityLevels = threadCounts.length;
				this.overflowScheduler = overflowScheduler;

				//noinspection unchecked
				this.tiers = (Tier[])Array.newInstance(Tier.class,numPriorityLevels);
				List<Tier> allTiers = Lists.newArrayListWithCapacity(numPriorityLevels);
				for(int i=tiers.length-1;i>=0;i--){
						List<Tier> higherTiers = Lists.newArrayList(allTiers);
						Tier tier = new Tier(i,threadCounts[i],higherTiers);
						tiers[i] = tier;
						allTiers.add(tier);
				}
		}

		public void shutdown(){
				for(Tier tier:tiers){
					tier.shutdown();
				}
		}

		@Override
		public TaskFuture submit(T task) throws ExecutionException {
				int priority = task.getPriority();
				if(priority>=numPriorityLevels){
						priority = numPriorityLevels/2;
				}
				Tier tier = tiers[priority];
				if(tier.trySubmit(task))
						return new ListeningTaskFuture<T>(task,tier.stats.numPending.get());

				for(int lowerP=0;lowerP<priority;lowerP++){
						tier = tiers[lowerP];
						if(tier.trySubmit(task)){
								return new ListeningTaskFuture<T>(task,tier.stats.numPending.get());
						}
				}
				//we were unable to submit it to any level below us--make the overflow rejectionHandler decide what to do
				OverflowHandler.OverflowPolicy overflowPolicy = overflowHandler.shouldOverflow(task);
				switch (overflowPolicy) {
						case REJECT:
								rejectionHandler.rejected(task);
								return null;
						case OVERFLOW:
								return overflowScheduler.submit(task);
						default:
								tier = tiers[priority];
								tier.forceQueue(task);
								return new ListeningTaskFuture<T>(task,tier.stats.numPending.get());
				}
		}

		public void setNumThreads(int priorityLevel,int newThreadCount){
				Preconditions.checkArgument(priorityLevel<tiers.length,"Priority level "+ priorityLevel+" is too high. Maximum priority is "+ tiers.length);
				Preconditions.checkArgument(priorityLevel>=0,"Priority level must be a non-negative number");
				tiers[priorityLevel].setNumThreads(newThreadCount);
		}

		private class Tier{
				private final ThreadPoolExecutor tierExecutor;
				public final TierStats stats = new TierStats();

				private final AtomicInteger queueSize = new AtomicInteger(0);
				private final BlockingDeque<T> outstandingTasks = new LinkedBlockingDeque<T>();
				private final List<Future<?>> tasks;
				private final List<Tier> higherTiers;

				@SuppressWarnings("unchecked")
				public Tier(int tierId,int numThreads,List<Tier> higherTiers){
						this.higherTiers = higherTiers;
						ThreadFactory factory = new ThreadFactoryBuilder()
										.setNameFormat("tier-"+tierId+"-thread-%d")
										.setUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
												@Override
												public void uncaughtException(Thread t, Throwable e) {
														WORKER_LOG.error("Uncaught error on thread "+ t.getName(),e);
												}
										}).build();
						this.tierExecutor = new ThreadPoolExecutor(numThreads, numThreads,
										60l, TimeUnit.MILLISECONDS,
										new LinkedBlockingQueue<Runnable>(),factory);
						tierExecutor.allowCoreThreadTimeOut(true);

						this.tasks = Lists.newArrayListWithCapacity(numThreads);
						for(int i=0;i<numThreads;i++){
								tasks.add(tierExecutor.submit(new Worker(this, higherTiers)));
						}
				}

				public boolean trySubmit(T task){
						if(!queueSize.compareAndSet(0,1))
								return false;

						stats.numPending.incrementAndGet();
						stats.tasksSubmitted.incrementAndGet();
						outstandingTasks.offer(task);
						return true;
				}


				public void forceQueue(T task){
						queueSize.incrementAndGet();
						stats.numPending.incrementAndGet();
						stats.tasksSubmitted.incrementAndGet();
						outstandingTasks.offer(task);
				}

				public void setNumThreads(int threadCount){
						tierExecutor.setMaximumPoolSize(threadCount);
						tierExecutor.setCorePoolSize(threadCount);

						/*
						 * If we are setting the pool size smaller, make sure and
						 * cancel running threads.
 					   */
						while(tasks.size()>threadCount){
								tasks.remove(0).cancel(true);
						}

						/*
						 * If we are setting the pool size higher, add new workers
						 * until we have the appropriate number of threads
						 */
						while(tasks.size()<threadCount){
								tasks.add(tierExecutor.submit(new Worker(this,higherTiers)));
						}
				}

				public void shutdown() {
						//cancel all running tasks
						for(Future<?> future:tasks){
								future.cancel(true);
						}
						tierExecutor.shutdownNow();
				}
		}

		private class TierStats implements TaskStatus.StatusListener{
				public final AtomicInteger tasksSubmitted = new AtomicInteger(0);
				public final AtomicInteger completedCount = new AtomicInteger(0);
				public final AtomicInteger failedCount = new AtomicInteger(0);
				public final AtomicInteger cancelledCount = new AtomicInteger(0);
				public final AtomicInteger invalidatedCount = new AtomicInteger(0);
				public final AtomicInteger numExecuting = new AtomicInteger(0);
				public final AtomicInteger numPending = new AtomicInteger(0);

				@Override
				public void statusChanged(Status oldStatus, Status newStatus, TaskStatus taskStatus) {
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
								case PENDING:
										numPending.incrementAndGet();
										return;
								case FAILED:
										failedCount.incrementAndGet();
										taskStatus.detachListener(this);
										return;
								case COMPLETED:
										completedCount.incrementAndGet();
										taskStatus.detachListener(this);
										return;
								case CANCELLED:
										cancelledCount.incrementAndGet();
										taskStatus.detachListener(this);
								case INVALID:
										invalidatedCount.incrementAndGet();
										taskStatus.detachListener(this);
										return;
								case EXECUTING:
										numExecuting.incrementAndGet();
						}
				}
		}

		private static final Logger WORKER_LOG = Logger.getLogger(BoundTierTaskScheduler.Worker.class);
		private class Worker implements Runnable{
				private final Tier mainTier;
				private final List<Tier> higherTiers;

				private Worker( Tier mainTier,
											 List<Tier> higherTiers) {
						this.mainTier = mainTier;
						this.higherTiers = higherTiers;
				}

				@Override
				public void run() {
						/*
						 * There are three stages:
						 *
						 * 1. Take items off my queue
						 * 2. Steal items from other queues within the same priority level
						 * 3. Steal items from other queues within higher priority levels
						 *
						 * First, we attempt to take items off of our queue. If there are
						 * no items on that queue, we attempt to take it off the back of
						 * an adjacent queue. If that succeeds, execute it, then begin the
						 * process over again. If it does not find anything, try subsequent
						 * adjacent queues until either a task is found or the entire level
						 * has been checked.
						 *
						 * If the entire level has been checked, and there is no work
						 * at this level, then begin the stealing process again at the highest
						 * possible level. If there is a task there, execute and restart, otherwise,
						 * continue down the priority levels until reaching this level.
						 *
						 * If we reach this level without finding any work, then we are without
						 * work to be done, so wait on our queue until something is added or
						 * a timeout is reached, at which point we will begin again.
						 */
						while(!Thread.currentThread().isInterrupted()){
								if(WORKER_LOG.isTraceEnabled())
										WORKER_LOG.trace("Looking for new work");
								T next = mainTier.outstandingTasks.poll();
								BlockingQueue<T> queue = mainTier.outstandingTasks;
								if(next==null){
										WORKER_LOG.trace("No work found, attempting to steal from overflow queue");
										next = overflowScheduler.steal();
										queue = null;
								}
								int pos=0;
								Tier nextTier;
								while(next==null && pos<higherTiers.size()){
										nextTier = higherTiers.get(pos);
										queue = nextTier.outstandingTasks;
										if(WORKER_LOG.isTraceEnabled())
												WORKER_LOG.trace("Attempting to steal from higher tier");
										next = queue.poll();
										pos++;
								}

								if(next==null){
										if(WORKER_LOG.isTraceEnabled())
												WORKER_LOG.trace("No tasks available to steal, waiting on tier queue");
										//we were STILL unable to find a task. Time to block on our own queue for a while
										queue = mainTier.outstandingTasks;
										try {
												next = queue.poll(200, TimeUnit.MILLISECONDS);
										} catch (InterruptedException e) {
												//we were told to shut down
												//mark the current thread as interrupted to ensure
												//that we break from the loop
												Thread.currentThread().interrupt();
										}
								}
								if(next!=null){
										if(WORKER_LOG.isTraceEnabled())
												WORKER_LOG.trace("Executing task");
										execute(next, queue);
								}
						}
				}

				private void execute(T next,BlockingQueue<T> sourceQueue) {
						try {
								next.getTaskStatus().attachListener(mainTier.stats);
								new TaskCallable<T>(next).call();
						}catch(InterruptedException ie){
							//we've been told to cancel, so put the task back on it's queue
								resetToQueue(next, sourceQueue);
						} catch (Exception e) {
								WORKER_LOG.error("Unexpected exception calling task " + Bytes.toString(next.getTaskId()), e);
						} finally{
								//reset to allow other threads to see that I'm not working on anything
								next.getTaskStatus().detachListener(mainTier.stats);
						}
				}

				private void resetToQueue(T next, BlockingQueue<T> sourceQueue) {
						try{
						if(sourceQueue==null){
								//it was stolen from the overflow tier, put it back
								overflowScheduler.submit(next);
						}else
								sourceQueue.offer(next);
						Thread.currentThread().interrupt();
						}catch(ExecutionException e){
								WORKER_LOG.error("Unable to restore task " + Bytes.toString(next.getTaskId())+ " after interrupt", e);
						}
				}
		}

}
