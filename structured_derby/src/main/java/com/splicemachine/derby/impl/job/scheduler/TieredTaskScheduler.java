package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.collect.Lists;
import com.splicemachine.job.Task;
import com.splicemachine.job.TaskFuture;
import com.splicemachine.job.TaskScheduler;

import javax.management.*;
import java.lang.reflect.Array;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bounded, Priority-based TaskScheduler.
 *
 * <p>Task Scheduling is done using a series of <em>priority tiers</em>, with
 * each level being handled independently by a TaskSchduler. When a level is idle,
 * it is able to steal work from higher-priority tiers, and a tier which is too
 * busy will attempt to push work on to lower-priority tiers.</p>
 *
 * <h2>Priority Levels</h2>
 * <p>This scheduler assumes that priorities are distributed according to the
 * distribution given by the {@link TieredTaskSchedulerSetup} which is given
 * at construction time. The assumption is that the setup splits a contiguous interval
 * of priorities (generally [0-Pm) where Pm is a maximum priority number).</p>
 *
 * <p>This implementation then takes this distribution, and for each contiguous band,
 * allocated a single tier. Thus, if the Setup splits the interval [0,100) into 4 equal
 * segments, there will be a tier for each segment. Each tier has its own scheduler
 * which operates independently of all other tiers.</p>
 *
 * <h2>Tier Saturation</h2>
 * <p>When too many tasks are submitted for the tier to execute all of them concurrently,
 * the tier is said to be <em>saturated</em>. When this occurs, this implementation will
 * attempt to find a lower-priority tier which is not saturated. If such a tier can be
 * found, then the task is submitted to that tier instead. The language for this
 * action is that a higher-priority tier <em>shrugged</em> its work on to the lower-priority
 * tier.</p>
 *
 * <p>Note that a tier will shrug tasks <em>only</em> to tiers with a lower priority. It will
 * <em>never</em> shrug work to higher-priority tiers.</p>
 *
 * <p>If the assigned tier <em>and</em> all lower-priority tiers are saturated, then the task
 * is considered to have <em>overflowed</em>. In this case, this implementation delegates
 * decisions to an {@link OverflowHandler}, which allows customized overflow behavior. </p>
 *
 * <h3>Task Overflow</h3>
 * <p>When a task overflows, then the overflow handler has 3 choices: {@code ENQUEUE},
 * {@code OVERFLOW}, and {@code REJECT}.</p>
 *
 * <p>If {@code ENQUEUE} is chosen, then the task is forcibly submitted to the <em>original</em>
 * tier for future processing. This means that the task must wait for available threads before
 * it can be processed.</p>
 *
 * <p>If {@code REJECT} is chosen, then the task is <em>rejected</em>, and the caller-specified
 * {@link RejectionHandler} is used to determine the correct behavior.</p>
 *
 * <p>If {@code OVERFLOW} is chosen, the task is submitted to an <em>overflow scheduler</em>, which
 * is a task scheduler which only accepts overflow tasks (and could potentially have strong restriction
 * on how many tasks are concurrently processed. This allows one to prevent deadlocks in hierarchically-arranged
 * tasks.</p>
 *
 * <h2>Work Stealing</h2>
 * <p>At any point where a tier has idle workers with nothing to do, that tier will attempt to
 * <em>steal</em> work from higher priority tiers which are saturated. </p>
 *
 * <p>Note that a tier can <em>never</em> steal work from a lower-priority tier. This prevents
 * priority inversion from occuring.</p>
 *
 * <h2>Computing maximum number of Tasks that can run for a given priority</h2>
 * <p>Because of the work-stealing and work-shrugging relationships allowed by this implementation,
 * it is somewhat complicated to determing the maximum number of threads available to execute a given
 * priority value.</p>
 *
 * <p>Suppose that there are {@code N} tiers, each with {@code W(i)} total workers, and that the priorities
 * are distributed as {@code t(1),t(2),...t(N)}, with {@code t(i) &lt;t(i+1)}. Then the maximum number of workers
 * available to execute a task with priority {@code t(i)&lt;=P&lt;t(i+1)} for some {@code i} is the sum of the workers
 * for all tiers lower = {@code sum {W(j) |j&lt;=i} }.</p>
 *
 *
 * <h2>Prioritization</h2>
 * Tasks are ordered according to their {@code getPriority()} method, with a smaller
 * priority number corresponding to a higher priority task. Thus, if Task 1 has a priority
 * of 10, and Task 2 has a priority of 12, then Task 1 is a higher priority task than Task
 *  2.
 *
 * <p>This implementation is configured in such a way as that tasks which have a different priority
 * but fall on the same priority tier will still be ordered in the same way. Thus, in the previous example,
 * if both Task 1 and Task 2 have the same priority tier, Task 1 will still be considered a higher priority
 * than Task 2 even within the same tier.</p>
 *
 * @author Scott Fines
 * Date: 12/8/13
 */
public class TieredTaskScheduler<T extends Task> implements TaskScheduler<T> {

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

		private final Tier[] tiers;
		private final StealableTaskScheduler<T> overflowScheduler;
		private final OverflowHandler overflowHandler;

		private final RejectionHandler<T> rejectionHandler;

		private final Stats stats = new Stats();

		public TieredTaskScheduler(TieredTaskSchedulerSetup schedulerSetup,
															 OverflowHandler overflowHandler,
															 StealableTaskScheduler<T> overflowScheduler){
				this(schedulerSetup, overflowHandler, overflowScheduler, ExceptionRejectionHandler.<T>instance());
		}
		public TieredTaskScheduler(TieredTaskSchedulerSetup schedulerSetup,
															 OverflowHandler overflowHandler,
															 StealableTaskScheduler<T> overflowScheduler,
															 RejectionHandler<T> rejectionHandler) {
				this.overflowScheduler = overflowScheduler;
				this.overflowHandler = overflowHandler;
				this.rejectionHandler = rejectionHandler;

				int []priorityLevels = schedulerSetup.getPriorityTiers();
				//noinspection unchecked
				this.tiers = (Tier[])Array.newInstance(Tier.class,priorityLevels.length);
				List<StealableTaskScheduler<T>> priorityList = Lists.newArrayListWithCapacity(priorityLevels.length);
				Comparator<T> comparator = new Comparator<T>() {
						@Override
						public int compare(T o1, T o2) {
								return o1.getPriority()-o2.getPriority();
						}
				};
				for(int i=0;i<priorityLevels.length;i++){
						int minPriority = priorityLevels[i];
						int numThreads = schedulerSetup.maxThreadsForPriority(minPriority);
						long pollTime = schedulerSetup.pollTimeForPriority(minPriority);
						List<StealableTaskScheduler<T>> higherPriority = Lists.newArrayList(priorityList);
						higherPriority.add(0,overflowScheduler);
						WorkStealingTaskScheduler<T> scheduler = new WorkStealingTaskScheduler<T>(numThreads,
										pollTime,
										comparator,
										higherPriority,
										"priority-tier-"+minPriority);
						tiers[i] = new Tier(minPriority,scheduler);
						priorityList.add(scheduler);
				}
		}

		public void shutdown(){
				for(Tier tier:tiers){
						tier.scheduler.shutdown();
				}
		}

		@Override
		public TaskFuture submit(T task) throws ExecutionException {
				/*
				 * First, attempt to submit to the proper priority level for this task. If
				 * that doesn't succeed (e.g. that task level is saturated), then attempt to submit
				 * it at a lower priority level. If all lower priority levels are saturated, then
				 * refer to the overflow handler to decide what to do.
				 */
				int priority = task.getPriority();
				int level=0;
				while(level < tiers.length-1 && tiers[level+1].priorityLevel<=priority){
						level++;
				}
				Tier levelTier = tiers[level];
				TaskFuture future  = levelTier.scheduler.tryExecute(task);
				if(future!=null) return future; //successfully submitted

				//the assigned level is saturated, try shrugging to lower priority levels
				for(int lowerLevel = tiers.length-1;lowerLevel>level;lowerLevel--){
						Tier shrugTier = tiers[lowerLevel];
						future = shrugTier.scheduler.tryExecute(task);
						if(future!=null){
								//we successfully shrugged from the minPriority level, so count it up and return
								levelTier.shrugCount.incrementAndGet();
								return future;
						}
				}
				//we can't push it anywhere, overflow
				return overflow(task, levelTier);
		}

		private TaskFuture overflow(T task, Tier baseTier) throws ExecutionException {
				/*
				 * We need to overflow this task.
				 *
				 * If it is rejected, call the rejection handler.
				 *
				 * If it overflows, push to the overflow scheduler
				 *
				 * if it enqueues then enqueue it to the base tier
				 */
				OverflowHandler.OverflowPolicy overflowPolicy = overflowHandler.shouldOverflow(task);
				switch (overflowPolicy) {
						case ENQUEUE:
								return baseTier.scheduler.submit(task);
						case OVERFLOW:
								return overflowScheduler.submit(task);
						default:
								stats.rejectedCount.incrementAndGet();
								rejectionHandler.rejected(task);
								return null;
				}
		}

		/**
		 * Register this implementation under JMX
		 *
		 * @param mbs the MBeanServer to use
		 * @throws MalformedObjectNameException
		 * @throws NotCompliantMBeanException
		 * @throws InstanceAlreadyExistsException
		 * @throws MBeanRegistrationException
		 */
		public void registerJMX(MBeanServer mbs) throws MalformedObjectNameException,
						NotCompliantMBeanException,
						InstanceAlreadyExistsException,
						MBeanRegistrationException {
				ObjectName name = new ObjectName("com.splicemachine.job:type=TieredSchedulerManagement");
				mbs.registerMBean(stats,name);

				for(Tier tier:tiers){
						tier.scheduler.registerJMX(mbs, "com.splicemachine.job.tasks.tier-" + tier.priorityLevel);
				}
		}

		/*
		 * Nice wrapper for management statistics information.
		 */
		private class Stats implements TieredSchedulerManagement{
				private final AtomicLong overflowCount = new AtomicLong(0l);
				private final AtomicLong rejectedCount = new AtomicLong(0l);

				@Override
				public long getTotalShruggedTasks() {
						long shrugCount = 0l;
						for(Tier tier:tiers){
							shrugCount+=tier.shrugCount.get();
						}
						return shrugCount;
				}

				@Override
				public long getTotalOverflowedTasks() {
						return overflowCount.get();
				}

				@Override
				public long getTotalStolenTasks() {
						long stolenCount = 0l;
						for(Tier tier:tiers){
								stolenCount+=tier.scheduler.getManagement().getTotalStolenTasks();
						}
						return stolenCount;
				}

				@Override
				public long getTotalSubmittedTasks() {
						long submittedCount = 0l;
						for(Tier tier:tiers){
								submittedCount+=tier.scheduler.getManagement().getTotalSubmittedTasks();
						}
						return submittedCount;
				}

				@Override
				public long getTotalCompletedTasks() {
						long completedCount = 0l;
						for(Tier tier:tiers){
								completedCount+=tier.scheduler.getManagement().getTotalCompletedTasks();
						}
						return completedCount;
				}

				@Override
				public long getTotalCancelledTasks() {
						long cancelledCount = 0l;
						for(Tier tier:tiers){
								cancelledCount+=tier.scheduler.getManagement().getTotalCancelledTasks();
						}
						return cancelledCount;
				}

				@Override
				public long getTotalFailedTasks() {
						long failedCount = 0l;
						for(Tier tier:tiers){
								failedCount+=tier.scheduler.getManagement().getTotalFailedTasks();
						}
						return failedCount;
				}

				@Override
				public long getTotalInvalidatedTasks() {
						long invalidatedCount = 0l;
						for(Tier tier:tiers){
								invalidatedCount+=tier.scheduler.getManagement().getTotalInvalidatedTasks();
						}
						return invalidatedCount;
				}

				@Override
				public long getExecuting() {
						long executingCount = 0l;
						for(Tier tier:tiers){
								executingCount+=tier.scheduler.getManagement().getNumExecutingTasks();
						}
						return executingCount;
				}

				@Override
				public long getPending() {
						long pendingCount = 0l;
						for(Tier tier:tiers){
								pendingCount+=tier.scheduler.getManagement().getNumPendingTasks();
						}
						return pendingCount;
				}

				@Override
				public int getTotalWorkerCount() {
						int workerCount = 0;
						for(Tier tier:tiers){
								workerCount+=tier.scheduler.getManagement().getCurrentWorkers();
						}
						return workerCount;
				}

				@Override
				public int getMostLoadedTier() {
						int maxRunning=0;
						int maxTier = 0;
						for(Tier tier:tiers){
								StealableTaskSchedulerManagement management = tier.scheduler.getManagement();
								int numExecutingTasks = management.getNumExecutingTasks()
												+ management.getNumPendingTasks();
								if(numExecutingTasks >maxRunning){
										maxRunning  = numExecutingTasks;
										maxTier = tier.priorityLevel;
								}
						}
						return maxTier;
				}

				@Override
				public int getLeastLoadedTier() {
						int minRunning=0;
						int minTier = 0;
						for(Tier tier:tiers){
								StealableTaskSchedulerManagement management = tier.scheduler.getManagement();
								int numExecutingTasks = management.getNumExecutingTasks()
												+ management.getNumPendingTasks();
								if(numExecutingTasks <minRunning){
										minRunning  = numExecutingTasks;
										minTier = tier.priorityLevel;
								}
						}
						return minTier;
				}
		}

		private class Tier{
				private final int priorityLevel;
				//TODO -sf- find a way to push this to the statistics
				private final AtomicLong shrugCount = new AtomicLong(0l);
				private final WorkStealingTaskScheduler<T> scheduler;

				private Tier(int priorityLevel, WorkStealingTaskScheduler<T> scheduler) {
						this.priorityLevel = priorityLevel;
						this.scheduler = scheduler;
				}
		}
}
