package com.splicemachine.derby.impl.job.scheduler;

/**
 * @author Scott Fines
 * Date: 11/27/13
 */
public interface CapacityPlanner<T> {

		/**
		 * @return the number of priority levels available
		 */
		int getNumLevels();

		/**
		 * @param availableThreads the number of threads available to be claimed
		 * @param priorityLevel a 0-indexed priority level
		 * @return the size of the planner for the given priority level
		 */
		int getPoolSize(int availableThreads, int priorityLevel);

		/**
		 * get the priority for the given task
		 * @param task the task to schedule
		 * @return the priority of that task
		 */
		int getPriority(T task);

		JobCapacity scheduleJob(String jobId);
}
