package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.base.Preconditions;

import java.util.Arrays;

/**
 * @author Scott Fines
 * Date: 12/6/13
 */
public class SchedulerSetups {

		private SchedulerSetups(){}

		/**
		 * Distributes threads uniformly over the specified tiers.
		 *
		 * <p>If the number of tiers does not evenly divide the number of threads, excess threads are evenly
		 * distributed from highest-tier down, so that higher tiers (tiers with a lower priority) may have more
		 * threads than lower tiers.</p>
		 *
		 * @param numThreads the number of threads to use
		 * @param numTiers the number of tiers to create
		 * @param maxPriority the maximum priority
		 * @return a setup with a uniform distribution of threads across evenly-spaced tiers.
		 */
		public static TieredTaskSchedulerSetup uniformSetup(int numThreads, int numTiers,int maxPriority){
				Preconditions.checkArgument(numTiers<=numThreads,"Insufficient threads to properly populate all tiers");

				int[] tiers = getEvenTiers(numTiers,maxPriority);
				int[] threadCounts = new int[numTiers];
				int threadsPerTier = numThreads/numTiers;
				Arrays.fill(threadCounts,threadsPerTier);

				int threadsRemaining = numThreads-(threadsPerTier*numTiers);
				int pos=numTiers-1;
				while(threadsRemaining>0){
						threadCounts[pos]++;
						threadsRemaining--;
						pos = (pos-1);
						if(pos<0)
								pos = numTiers-1;
				}
				return new PresetTieredTaskSchedulerSetup(tiers,threadCounts);
		}

		/**
		 * Distribute task threads over equal-sized tiers using a binary-normalized distribution scheme.
		 *
		 * <p>In a binary-normalized scheme, we begin with N threads and T tiers of equal size, and we consider
		 * the median tier. That tier will contain (N/2) threads. Then we take the remaining threads, and half
		 * of those are assigned to the next tier lower, then half the remaining assigned to the tier above, and
		 * so on until all tiers are assigned threads.</p>
		 *
		 * @param numThreads the total number of threads
		 * @param numTiers the number of tiers to distribute the threads over
		 * @param maxPriority the assumed maximum priority
		 * @return a TieredTaskSchedulerSetup with a binary-normalized distribution favoring middle and lower
		 * tiers over higher tiers.
		 */
		public static TieredTaskSchedulerSetup binaryNormalizedSetup(int numThreads, int numTiers, int maxPriority){
				Preconditions.checkArgument(numTiers<=numThreads,"Insufficient threads to properly populate all tiers");
				//divide the range [o,maxPriority) into numTiers ranges
				int[] tierPriorityLevels = getEvenTiers(numTiers, maxPriority);

				int[] threadCounts = new int[numTiers];
				Arrays.fill(threadCounts,1); //guarantee at least one thread per tier

				int threadsRemaining = numThreads-numTiers;
				int pos = numTiers/2;
				if(numTiers%2==0)
						pos--;
				int shift = pos>=numTiers||pos==0?-1:1;
				for(int visited=0;visited<numTiers && threadsRemaining>0;visited++){
						int next = threadsRemaining<2||visited==numTiers-1? threadsRemaining: threadsRemaining / 2;
						threadCounts[pos]+=next;
						threadsRemaining-=next;

						pos+=shift;
						if(shift<0)
								shift--;
						else
							shift++;
						shift *=-1;
						if(pos+shift>=numTiers){
								//we've run off the bottom of the setup, so just change shift to increment after that
								shift=-1;
						}
				}

				return new PresetTieredTaskSchedulerSetup(tierPriorityLevels,threadCounts);
		}

		private static int[] getEvenTiers(int numTiers, int maxPriority) {
				int[] tierPriorityLevels = new int[numTiers];
				int tierSize = maxPriority/numTiers;
				for(int i=0,priorityLevel=0;i<tierPriorityLevels.length;i++,priorityLevel+=tierSize){
						tierPriorityLevels[i] = priorityLevel;
				}
				return tierPriorityLevels;
		}

		public static void main(String... args)throws Exception{
				TieredTaskSchedulerSetup setup = binaryNormalizedSetup(10, 3, 100);
				int[] priorityTiers = setup.getPriorityTiers();
				System.out.println(Arrays.toString(priorityTiers));
				for (int priorityTier : priorityTiers) {
						System.out.printf("%d:%d%n", priorityTier, setup.maxThreadsForPriority(priorityTier));
				}
		}
}
