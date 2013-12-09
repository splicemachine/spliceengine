package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.collect.Lists;
import com.splicemachine.job.Task;
import com.splicemachine.job.TaskFuture;
import com.splicemachine.job.TaskScheduler;

import java.lang.reflect.Array;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;

/**
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
		private final AtomicLong totalRejectedTasks = new AtomicLong(0l);

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
				for(int i=0;i<priorityLevels.length;i++){
						int minPriority = priorityLevels[i];
						int numThreads = schedulerSetup.maxThreadsForPriority(minPriority);
						long pollTime = schedulerSetup.pollTimeForPriority(minPriority);
						List<StealableTaskScheduler<T>> higherPriority = Lists.newArrayList(priorityList);
						higherPriority.add(0,overflowScheduler);
						WorkStealingTaskScheduler<T> scheduler = new WorkStealingTaskScheduler<T>(numThreads,
										pollTime,higherPriority,
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
				while(level < tiers.length-1 && tiers[level+1].priorityLevel<priority){
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

				// we were unable to find a level that isn't saturated--call the overflow handler
				OverflowHandler.OverflowPolicy overflowPolicy = overflowHandler.shouldOverflow(task);
				switch (overflowPolicy) {
						case ENQUEUE:
								return levelTier.scheduler.submit(task);
						case OVERFLOW:
								return overflowScheduler.submit(task);
						default:
								totalRejectedTasks.incrementAndGet();
								rejectionHandler.rejected(task);
								return null;
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
