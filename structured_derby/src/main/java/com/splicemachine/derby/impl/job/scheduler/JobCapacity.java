package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

/**
 *
 * @author Scott Fines
 * Date: 11/27/13
 */
class JobCapacity {

		private final List<String> runningJobTasks = Collections.synchronizedList(Lists.<String>newArrayList());
		private final int maxTasksPerJob;

		JobCapacity(int maxTasksPerJob) { this.maxTasksPerJob = maxTasksPerJob; }

		public boolean canRun(String jobId){
				int count=0;
				for(String runningJob:runningJobTasks){
						if(runningJob.equals(jobId))
							count++;
				}
				return count < maxTasksPerJob;
		}

		public synchronized boolean taskStarted(String jobId){
				if(!canRun(jobId))
						return false;
				runningJobTasks.add(jobId);
				return true;
		}

		public void taskFinished(String jobId){
			runningJobTasks.remove(jobId);
		}
}
