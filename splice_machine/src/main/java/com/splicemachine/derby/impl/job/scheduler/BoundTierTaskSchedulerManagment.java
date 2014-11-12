package com.splicemachine.derby.impl.job.scheduler;

import javax.management.MXBean;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 12/6/13
 */
@MXBean
public interface BoundTierTaskSchedulerManagment {

		int getNumTiers();

		long getTotalTasksStolen();

		Map<Integer,Long> getTasksStolen();

		long getTotalTasksShrugged();

		Map<Integer,Long> getTasksShrugged();

		int getTotalWorkers();
		/**
		 * @return the number of workers for each priority tier
		 */
		Map<Integer,Integer> getWorkerCount();

		void setWorkerCount(int priorityLevel, int newWorkerCount);

		long getTotalSubmittedTasks();

		Map<Integer,Long> getSubmittedTasks();

		int getTotalPendingTasks();

		Map<Integer,Integer> getPendingTasks();

		int getTotalExecutingTasks();

		Map<Integer,Integer> getExecutingTasks();

		long getTotalCompletedTasks();

		Map<Integer,Long> getCompletedTasks();

		long getTotalFailedTasks();

		Map<Integer,Long> getFailedTasks();

		long getTotalCancelledTasks();

		Map<Integer,Long> getCancelledTasks();

		long getTotalInvalidatedTasks();

		Map<Integer,Long> getInvalidatedTasks();

		long getTotalOverflowedTasks();

		long getTotalRejectedTasks();

}
