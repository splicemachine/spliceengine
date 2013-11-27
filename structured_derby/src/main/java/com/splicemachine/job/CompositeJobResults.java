package com.splicemachine.job;

import com.google.common.collect.Lists;
import com.splicemachine.derby.stats.TaskStats;
import org.apache.derby.iapi.error.StandardException;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 11/20/13
 */
public class CompositeJobResults implements JobResults{
		private static final Logger logger = Logger.getLogger(CompositeJobResults.class);
		private final List<JobFuture> jobFutures;
		private final JobStats compositeStats;

		public CompositeJobResults(List<JobFuture> jobFutures,List<JobStats> stats, long totalTime) {
				this.jobFutures = jobFutures;
				this.compositeStats = new CompositeJobStats(stats,totalTime);
		}

		@Override
		public JobStats getJobStats() {
				return compositeStats;
		}

		@Override
		public void cleanup() throws StandardException {
				for(JobFuture future:jobFutures){
						try {
								future.cleanup();
						} catch (ExecutionException e) {
								logger.error(e);
						}
				}
		}

		private static class CompositeJobStats implements JobStats {
				private final List<JobStats> stats;
				private final long totalTime;

				public CompositeJobStats(List<JobStats> stats, long totalTime) {
						this.stats= stats;
						this.totalTime = totalTime;
				}

				@Override
				public int getNumTasks() {
						int numTasks=0;
						for(JobStats stat:stats){
								numTasks+=stat.getNumTasks();
						}
						return numTasks;
				}

				@Override
				public long getTotalTime() {
						return totalTime;
				}

				@Override
				public int getNumSubmittedTasks() {
						int numTasks=0;
						for(JobStats stat:stats){
								numTasks+=stat.getNumSubmittedTasks();
						}
						return numTasks;
				}

				@Override
				public int getNumCompletedTasks() {
						int numTasks=0;
						for(JobStats stat:stats){
								numTasks+=stat.getNumCompletedTasks();
						}
						return numTasks;
				}

				@Override
				public int getNumFailedTasks() {
						int numTasks=0;
						for(JobStats stat:stats){
								numTasks+=stat.getNumFailedTasks();
						}
						return numTasks;
				}

				@Override
				public int getNumInvalidatedTasks() {
						int numTasks=0;
						for(JobStats stat:stats){
								numTasks+=stat.getNumInvalidatedTasks();
						}
						return numTasks;
				}

				@Override
				public int getNumCancelledTasks() {
						int numTasks=0;
						for(JobStats stat:stats){
								numTasks+=stat.getNumCancelledTasks();
						}
						return numTasks;
				}

				@Override
				public List<TaskStats> getTaskStats() {
						List<TaskStats> allTaskStats = new ArrayList<TaskStats>();
						for(JobStats stat:stats){
								allTaskStats.addAll(stat.getTaskStats());
						}
						return allTaskStats;
				}

				@Override
				public String getJobName() {
						return "multiScanJob"; //TODO -sf- use a better name here
				}

				@Override
				public List<byte[]> getFailedTasks() {
						List<byte[]> failedTasks = Lists.newArrayList();
						for(JobStats stat:stats){
								failedTasks.addAll(stat.getFailedTasks());
						}
						return failedTasks;
				}
		}
}
