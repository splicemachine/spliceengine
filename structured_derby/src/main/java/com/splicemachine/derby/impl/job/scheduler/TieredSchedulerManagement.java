package com.splicemachine.derby.impl.job.scheduler;

import javax.management.MXBean;

/**
 * @author Scott Fines
 * Date: 12/8/13
 */
@MXBean
public interface TieredSchedulerManagement {
		long getTotalShruggedTasks();
		long getTotalOverflowedTasks();
		long getTotalStolenTasks();

		long getTotalSubmittedTasks();
		long getTotalCompletedTasks();
		long getTotalCancelledTasks();
		long getTotalFailedTasks();
		long getTotalInvalidatedTasks();

		long getExecuting();
		long getPending();

		/**
		 * @return the total number of workers.
		 */
		int getTotalWorkerCount();

		/**
		 * @return the tier which currently has the most tasks
		 * to execute. Includes tasks which are pending <em>and</em>
		 * executing.
		 */
		int getMostLoadedTier();

		/**
		 * @return the tier which currently has the fewest tasks
		 * to execute. Includes tasks which are pending <em>and</em>
		 * executing.
		 */
		int getLeastLoadedTier();
}
