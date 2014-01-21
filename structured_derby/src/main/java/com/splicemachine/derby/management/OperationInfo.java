package com.splicemachine.derby.management;

import com.splicemachine.derby.impl.job.JobInfo;

import java.beans.ConstructorProperties;

/**
 * @author Scott Fines
 * Date: 1/21/14
 */
public class OperationInfo {
		private final long operationUuid;
		private final String operationTypeName;
		private final long parentOperationUuid; //-1 for no parent
		private volatile int numJobs;
		private volatile int numTasks;
		private volatile int numServers; //TODO -sf- get this metric somehow

		public OperationInfo(long operationUuid,
												 String operationTypeName,
												 long parentOperationUuid) {
				this.operationUuid = operationUuid;
				this.operationTypeName = operationTypeName;
				this.parentOperationUuid = parentOperationUuid;
				this.numJobs=0;
				this.numTasks=0;
				this.numServers=0;
		}

		@ConstructorProperties({"numServers","numTasks","numJobs","parentOperationUuid","operationTypeName","operationUuid"})
		public OperationInfo(int numServers, int numTasks, int numJobs,
												 long parentOperationUuid, String operationTypeName, long operationUuid) {
				this.numServers = numServers;
				this.numTasks = numTasks;
				this.numJobs = numJobs;
				this.parentOperationUuid = parentOperationUuid;
				this.operationTypeName = operationTypeName;
				this.operationUuid = operationUuid;
		}

		public long getOperationUuid() { return operationUuid; }
		public String getOperationTypeName() { return operationTypeName; }
		public long getParentOperationUuid() { return parentOperationUuid; }
		public int getNumJobs() { return numJobs; }
		public void setNumJobs(int numJobs) { this.numJobs = numJobs; }
		public int getNumTasks() { return numTasks; }
		public void setNumTasks(int numTasks) { this.numTasks = numTasks; }
		public int getNumServers() { return numServers; }
		public void setNumServers(int numServers) { this.numServers = numServers; }

		public void addJob(JobInfo jobInfo){
				this.numJobs++;
				this.numTasks+=jobInfo.totalTaskCount();
		}


		@Override
		public boolean equals(Object o) {
				if (this == o) return true;
				if (!(o instanceof OperationInfo)) return false;
				OperationInfo that = (OperationInfo) o;

				return operationUuid == that.operationUuid;
		}

		@Override
		public int hashCode() {
				return (int) (operationUuid ^ (operationUuid >>> 32));
		}
}
