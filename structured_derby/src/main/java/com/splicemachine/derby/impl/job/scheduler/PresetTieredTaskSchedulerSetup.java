package com.splicemachine.derby.impl.job.scheduler;

/**
 * @author Scott Fines
 *         Date: 12/6/13
 */
public class PresetTieredTaskSchedulerSetup implements TieredTaskSchedulerSetup {
		private final int[] priorityLevels;
		private final int[] threads;

		private final long pollTimeMs;

		public PresetTieredTaskSchedulerSetup(int[] priorityLevels, int[] threads) {
				this(priorityLevels, threads,200l);
		}

		public PresetTieredTaskSchedulerSetup(int[] priorityLevels, int[] threads, long pollTimeMs) {
				this.priorityLevels = priorityLevels;
				this.threads = threads;
				this.pollTimeMs = pollTimeMs;
		}

		@Override
		public int[] getPriorityTiers() {
				return priorityLevels;
		}

		@Override
		public int maxThreadsForPriority(int minPriorityForTier) {
				int pos=-1;
				for(int priorityLevel:priorityLevels){
						if(priorityLevel>minPriorityForTier)
								break;
						else
								pos++;
				}
				return threads[pos];
		}

		@Override
		public long pollTimeForPriority(int minPriority) {
				return pollTimeMs;
		}
}
