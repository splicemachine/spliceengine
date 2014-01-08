package com.splicemachine.derby.impl.job;

import com.splicemachine.job.JobFuture;
import org.apache.hadoop.hbase.util.Bytes;

import javax.management.openmbean.*;
import java.beans.ConstructorProperties;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 * Date: 1/6/14
 */
public class JobInfo implements JobFuture.StatusHook {
		private final String jobId;

		private final long[] taskIds;
		private int taskPos = 0;
		private final long jobStartMs;
		private volatile long jobFinishMs;

		private final AtomicInteger tasksPending = new AtomicInteger(0);
		private final AtomicInteger tasksCompleted = new AtomicInteger(0);
		private final AtomicInteger tasksFailed = new AtomicInteger(0);
		private volatile boolean jobFailed;

		public JobInfo(String jobId,int numTasks,long jobStartMs) {
				this.jobId = jobId;

				this.jobStartMs = jobStartMs;
				this.taskIds = new long[numTasks];
		}

		@ConstructorProperties({"jobId","taskIds","jobStartMs","jobFinishMs","tasksPending","tasksCompleted","tasksFailed"})
		public JobInfo(String jobId, long[] taskIds, long jobStartMs, long jobFinishMs, int tasksPending, int tasksCompleted, int tasksFailed){
				this.jobId = jobId;
				this.taskIds = taskIds;
				this.jobStartMs = jobStartMs;
				this.jobFinishMs = jobFinishMs;
				this.tasksPending.set(tasksPending);
				this.tasksCompleted.set(tasksCompleted);
				this.tasksFailed.set(tasksFailed);
		}

		public String getJobId() { return jobId; }
		public long[] getTaskIds() { return taskIds; }
		public int getTaskPos() { return taskPos; }
		public long getJobStartMs() { return jobStartMs; }
		public long getJobFinishMs() { return jobFinishMs; }
		public int getTasksPending() { return tasksPending.get(); }
		public int getTasksCompleted() { return tasksCompleted.get(); }
		public int getTasksFailed() { return tasksFailed.get(); }

		public JobInfo(String jobId,int numTasks) {
				this.jobId = jobId;

				this.jobStartMs = System.currentTimeMillis();
				this.taskIds = new long[numTasks];
		}

		public void taskRunning(byte[] taskId){
				synchronized (taskIds){
						taskIds[taskPos] = Bytes.toLong(taskId);
						taskPos++;
				}
				tasksPending.incrementAndGet();
		}

		@Override
		public void success(byte[] taskId) {
				int completedCount = tasksCompleted.incrementAndGet();
				tasksPending.decrementAndGet();
				if(completedCount>=taskIds.length){
						//we are finished! whoo!
						jobFinishMs = System.currentTimeMillis();
				}
		}

		@Override
		public void failure(byte[] taskId) {
				tasksFailed.incrementAndGet();
		}

		@Override
		public void cancelled(byte[] taskId) {
			//no-op
		}

		@Override
		public void invalidated(byte[] taskId) {
			//no-op
		}

		public void failJob(){
				this.jobFailed = true;
		}

		public void tasksRunning(byte[][] allTaskIds) {
			for(byte[] tId:allTaskIds){
					taskRunning(tId);
			}
		}

		public boolean isJobFailed() {
				return jobFailed;
		}
}

