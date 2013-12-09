package com.splicemachine.derby.impl.job.scheduler;

import javax.management.MXBean;

/**
 * @author Scott Fines
 * Date: 12/8/13
 */
@MXBean
public interface StealableTaskSchedulerManagement {
		int getCurrentWorkers();
		void setCurrentWorkers(int maxWorkers);

		long getTotalCompletedTasks();
		long getTotalFailedTasks();
		long getTotalCancelledTasks();
		long getTotalInvalidatedTasks();

		int getNumExecutingTasks();
		int getNumPendingTasks();

		long getTotalStolenTasks();
		long getTotalSubmitFailures();
}
