package com.splicemachine.derby.management;

import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.si.api.Txn;
import org.apache.derby.iapi.tools.run;

import java.beans.ConstructorProperties;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.ExecutionException;

/**
 * Represents information about a SQL statement.
 *
 * @author Scott Fines
 * Date: 1/6/14
 */
public class StatementInfo {
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
		private final long txn;
		private volatile long stopTimeMs = -1l;
		private volatile boolean isCancelled;

		private final Set<OperationInfo> operationInfo;

		public StatementInfo(String sql,
												 String user,
												 TxnView txn,
												 int numSinks,
												 com.splicemachine.uuid.Snowflake uuidGenerator) {
				this.numSinks = numSinks;
				this.user = user;
				this.sql = sql;
        this.txn = txn.getTxnId();

        if(numSinks>0){
            runningJobIds = new CopyOnWriteArraySet<JobInfo>();
            completedJobIds = new CopyOnWriteArraySet<JobInfo>();
//            runningJobIds = Collections.newSetFromMap(new ConcurrentHashMap<JobInfo, Boolean>());
//            completedJobIds = Collections.newSetFromMap(new ConcurrentHashMap<JobInfo, Boolean>());
        }else{
            runningJobIds = completedJobIds = null;
        }
        this.operationInfo = new CopyOnWriteArraySet<OperationInfo>();
//        this.operationInfo = Collections.newSetFromMap(new ConcurrentHashMap<OperationInfo, Boolean>());

//				if(numSinks>0){
//						runningJobIds = Collections.newSetFromMap(new ConcurrentHashMap<JobInfo, Boolean>());
//						completedJobIds = Collections.newSetFromMap(new ConcurrentHashMap<JobInfo, Boolean>());
//				}else{
//						runningJobIds = completedJobIds = null;
//				}
//				this.operationInfo = Collections.newSetFromMap(new ConcurrentHashMap<OperationInfo, Boolean>());

				this.statementUuid = uuidGenerator.nextUUID();
				this.startTimeMs = System.currentTimeMillis();
		}

        public StatementInfo(String sql,
                             String user,
                             TxnView txn,
                             int numSinks,
                             long statementUuid) {
            this.numSinks = numSinks;
            this.user = user;
            this.sql = sql;
            this.txn = txn.getTxnId();

            if(numSinks>0){
                runningJobIds = Collections.newSetFromMap(new ConcurrentHashMap<JobInfo, Boolean>());
                completedJobIds = Collections.newSetFromMap(new ConcurrentHashMap<JobInfo, Boolean>());
            }else{
                runningJobIds = completedJobIds = null;
            }
            this.operationInfo = Collections.newSetFromMap(new ConcurrentHashMap<OperationInfo, Boolean>());

            this.statementUuid = statementUuid;
            this.startTimeMs = System.currentTimeMillis();
        }

		@ConstructorProperties({"sql","user","txnId","numJobs",
						"statementUuid","runningJobs","completedJobs",
						"startTimeMs","stopTimeMs","operationInfo"})
		public StatementInfo(String sql,String user,long txnId,
												 int numSinks,long statementUuid,
												 Set<JobInfo> runningJobs,
												 Set<JobInfo> completedJobs,
												 long startTimeMs,long stopTimeMs,
												 Set<OperationInfo> operationInfo){
				this.sql = sql;
				this.user = user;
				this.txn = txnId;
				this.statementUuid = statementUuid;
				this.numSinks = numSinks;
				this.runningJobIds = runningJobs;
				this.completedJobIds = completedJobs;
				this.startTimeMs = startTimeMs;
				this.stopTimeMs = stopTimeMs;
				this.operationInfo = operationInfo;
		}

		public void addRunningJob(long operationId,JobInfo jobInfo) throws ExecutionException {
				if(isCancelled)
						jobInfo.cancel();

				runningJobIds.add(jobInfo);
				for(OperationInfo info:operationInfo){
						if(info.getOperationUuid()==operationId){
								info.addJob(jobInfo);
						}
				}
		}

		public void completeJob(JobInfo jobInfo){
				completedJobIds.add(jobInfo);
				runningJobIds.remove(jobInfo);
		}

		public long getTxnId() { return txn; }
		public int getNumJobs(){ return numSinks;}
		public String getSql() { return sql; }
		public long getStatementUuid() { return statementUuid; }
		public Set<JobInfo> getRunningJobs() { return runningJobIds; }
		public Set<JobInfo> getCompletedJobs() { return completedJobIds; }
		public long getStartTimeMs() { return startTimeMs; }
		public long getStopTimeMs() { return stopTimeMs; }
		public String getUser() { return user; }

		public Set<OperationInfo> getOperationInfo() { return operationInfo; }
//		public Set<Long> getOperationInfo(){ return Sets.newHashSet(1l);}

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
				if(runningJobIds==null) return;

				for(JobInfo runningJob:runningJobIds){
						runningJob.cancel();
						if(completedJobIds!=null)
								completedJobIds.add(runningJob);
				}
				runningJobIds.clear();
		}

		public String status() {
				if(isCancelled) return "CANCELLED";
				if(completedJobIds==null) return "SUCCESS";

				for(JobInfo completeInfo:completedJobIds){
						switch(completeInfo.getJobState()){
								case CANCELLED:
										return "CANCELLED";
								case FAILED:
										return "FAILED";
								default: //left empty so that the doesn't loop warning doesn't appear
						}
				}
				return "SUCCESS";
		}

		public int numCancelledJobs() {
				int numCancelled=0;
				if(completedJobIds!=null){
						for(JobInfo info:completedJobIds){
								if(info.getJobState()== JobInfo.JobState.CANCELLED)
										numCancelled++;
						}
				}
				return numCancelled;
		}

		public int numFailedJobs() {
				int numFailed=0;
				if(completedJobIds!=null){
						for(JobInfo info:completedJobIds){
								if(info.getJobState()== JobInfo.JobState.FAILED)
										numFailed++;
						}
				}
				return numFailed;
		}

    public static Callable<Void> completeOnClose(final StatementInfo stInfo, final JobInfo jobInfo){
        return new Callable<Void>() {
            @Override
            public Void call() throws IOException {
                stInfo.completeJob(jobInfo);
								return null;
            }
        };
    }

		public int numSuccessfulJobs() {
				int numSuccess=0;
				if(completedJobIds!=null){
						numSuccess = completedJobIds.size();
						for(JobInfo info:completedJobIds){
								if(info.getJobState()!= JobInfo.JobState.COMPLETED)
										numSuccess--;
						}
				}
				return numSuccess;
		}

		public void setOperationInfo(List<OperationInfo> operationInfo) {
				this.operationInfo.addAll(operationInfo);
		}
}

