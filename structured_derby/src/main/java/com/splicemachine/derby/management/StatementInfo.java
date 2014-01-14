package com.splicemachine.derby.management;

import com.splicemachine.derby.hbase.SpliceDriver;
import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.utils.Snowflake;

import javax.management.openmbean.*;
import java.beans.ConstructorProperties;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;

/**
 * Represents information about a SQL statement.
 *
 * @author Scott Fines
 * Date: 1/6/14
 */
public class StatementInfo {
		/*true if the statement includes task lookups*/
		private final String sql;
		private final String user;
		private final int numSinks;

		/*
		 * Long-stored form of an 8-byte generated uuid. This
		 * statement is associated with a unique identifier.
		 *
		 * If 2 separate instances of the same prepared statement
		 * are being executed, there will be two distinct StatementInfo
		 * entities, which will have different uuids.
		 */
		private final long statementUuid;

		private final Set<JobInfo> runningJobIds;
		private final Set<JobInfo> completedJobIds;

		private final long startTimeMs;
		private final String txnId;
		private volatile long stopTimeMs = -1l;
		private volatile boolean isCancelled;

		public StatementInfo(String sql,
												 String user,
												 String txnId,
												 int numSinks,
												 Snowflake snowflake) {
				this.numSinks = numSinks;
				this.user = user;
				this.sql = sql;
				this.txnId = txnId;

				if(numSinks>0){
						runningJobIds = Collections.newSetFromMap(new ConcurrentHashMap<JobInfo, Boolean>());
						completedJobIds = Collections.newSetFromMap(new ConcurrentHashMap<JobInfo, Boolean>());
				}else{
						runningJobIds = completedJobIds = null;
				}

				this.statementUuid = snowflake.nextUUID();
				this.startTimeMs = System.currentTimeMillis();
		}

		@ConstructorProperties({"sql","user","txnId","numJobs","statementUuid","runningJobs","completedJobs","startTimeMs","stopTimeMs"})
		public StatementInfo(String sql,String user,String txnId,
												 int numSinks,long statementUuid,
												 Set<JobInfo> runningJobs, Set<JobInfo> completedJobs,long startTimeMs,long stopTimeMs){
				this.sql = sql;
				this.user = user;
				this.txnId = txnId;
				this.statementUuid = statementUuid;
				this.numSinks = numSinks;
				this.runningJobIds = runningJobs;
				this.completedJobIds = completedJobs;
				this.startTimeMs = startTimeMs;
				this.stopTimeMs = stopTimeMs;
		}

		public void addRunningJob(JobInfo jobInfo) throws ExecutionException {
				if(isCancelled)
						jobInfo.cancel();

				runningJobIds.add(jobInfo);
		}

		public void completeJob(JobInfo jobInfo){
				completedJobIds.add(jobInfo);
				runningJobIds.remove(jobInfo);
		}

		public String getTxnId() { return txnId; }
		public int getNumJobs(){ return numSinks;}
		public String getSql() { return sql; }
		public long getStatementUuid() { return statementUuid; }
		public Set<JobInfo> getRunningJobs() { return runningJobIds; }
		public Set<JobInfo> getCompletedJobs() { return completedJobIds; }
		public long getStartTimeMs() { return startTimeMs; }
		public long getStopTimeMs() { return stopTimeMs; }
		public String getUser() { return user; }

		public void markCompleted(){ this.stopTimeMs = System.currentTimeMillis(); }
		@Override
		public boolean equals(Object o) {
				if (this == o) return true;
				if (o == null || getClass() != o.getClass()) return false;

				StatementInfo that = (StatementInfo) o;

				return statementUuid == that.statementUuid;
		}

		@Override
		public int hashCode() {
				return (int) (statementUuid ^ (statementUuid >>> 32));
		}

		public boolean isComplete() {
				return stopTimeMs>0l;
		}

		public void cancel() throws ExecutionException {
				isCancelled=true;
				for(JobInfo runningJob:runningJobIds){
						runningJob.cancel();
						completedJobIds.add(runningJob);
				}
				runningJobIds.clear();
		}
}

