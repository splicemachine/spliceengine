package com.splicemachine.derby.impl.job.scheduler;

/**
 * @author Scott Fines
 * Date: 12/5/13
 */
public interface TieredTaskSchedulerSetup {

		int[] getPriorityTiers();

		int maxThreadsForPriority(int minPriorityForTier);

}
