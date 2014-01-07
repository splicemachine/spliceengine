package com.splicemachine.derby.management;

import com.splicemachine.derby.impl.job.JobInfo;
import com.splicemachine.utils.Snowflake;

import javax.management.openmbean.*;
import java.beans.ConstructorProperties;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

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
		private volatile long stopTimeMs = -1l;

		public StatementInfo(String sql,
												 String user,
												 int numSinks,
												 Snowflake snowflake) {
				this.numSinks = numSinks;
				this.user = user;
				this.sql = sql;

				if(numSinks>0){
						runningJobIds = Collections.newSetFromMap(new ConcurrentHashMap<JobInfo, Boolean>());
						completedJobIds = Collections.newSetFromMap(new ConcurrentHashMap<JobInfo, Boolean>());
				}else{
						runningJobIds = completedJobIds = null;
				}

				this.statementUuid = snowflake.nextUUID();
				this.startTimeMs = System.currentTimeMillis();
		}

		@ConstructorProperties({"sql","user","numJobs","statementUuid","runningJobs","completedJobs","startTimeMs","stopTimeMs"})
		public StatementInfo(String sql,String user,int numSinks,long statementUuid,Set<JobInfo> runningJobs, Set<JobInfo> completedJobs,long startTimeMs,long stopTimeMs){
				this.sql = sql;
				this.user = user;
				this.statementUuid = statementUuid;
				this.numSinks = numSinks;
				this.runningJobIds = runningJobs;
				this.completedJobIds = completedJobs;
				this.startTimeMs = startTimeMs;
				this.stopTimeMs = stopTimeMs;
		}

		public void addRunningJob(JobInfo jobInfo){
				runningJobIds.add(jobInfo);
		}

		public void completeJob(JobInfo jobInfo){
				completedJobIds.add(jobInfo);
				runningJobIds.remove(jobInfo);
		}

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

//		@Override
//		public CompositeData toCompositeData(CompositeType ct) {
//				try {
//						List<String> itemNames = new ArrayList<String>(ct.keySet());
//						List<String> itemDescriptions = new ArrayList<String>(itemNames.size());
//						List<OpenType<?>> itemTypes = new ArrayList<OpenType<?>>(itemNames.size());
//
//						for(String itemName: itemNames){
//								itemDescriptions.add(ct.getDescription(itemName));
//								itemTypes.add(ct.getType(itemName));
//						}
//
//
//						CompositeType jobInfoType = new CompositeType(
//										"com.splicemachine.derby.impl.job.JobInfo","Job information",
//										new String[]{"jobId"},new String[]{"Unique Job identifier"},new OpenType[]{SimpleType.STRING});
//						Set<JobInfo> runningCopy = null;
//						if(runningJobIds!=null){
//								runningCopy = new HashSet<JobInfo>(runningJobIds);
//								if(runningCopy.size()>0){
//										itemNames.add("runningJobs");
//										itemDescriptions.add("Running Jobs");
//										itemTypes.add(new ArrayType(runningJobIds.size(),jobInfoType));
//								}
//						}
//
//						Set<JobInfo> completedCopy = null;
//						if(completedJobIds!=null){
//								completedCopy = new HashSet<JobInfo>(completedJobIds);
//								if(completedCopy.size()>0){
//										itemNames.add("completedJobs");
//										itemDescriptions.add("Completed Jobs");
//										itemTypes.add(new ArrayType(completedJobIds.size(),jobInfoType));
//								}
//						}
//
//						CompositeType xct = new CompositeType(ct.getTypeName(),
//										ct.getDescription(),
//										itemNames.toArray(new String[itemNames.size()]),
//										itemDescriptions.toArray(new String[itemDescriptions.size()]),
//										itemTypes.toArray(new OpenType[itemTypes.size()])
//										);
//						Object[] data;
//						String[] labels;
//						if(runningCopy!=null && runningCopy.size()>0){
//								CompositeData[] runningJobInfo = getJobInfoCompositeData(jobInfoType,completedCopy);
//								if(completedCopy!=null && completedCopy.size()>0){
//										CompositeData[] completedJobInfo = getJobInfoCompositeData(jobInfoType, completedCopy);
//										labels = new String[] {"statementUuid", "sql","numJobs",
//														"startTimeMs","stopTimeMs","runningJobs","completedJobs"};
//										data = new Object[] {statementUuid,sql,numSinks,startTimeMs,stopTimeMs,runningJobInfo,completedJobInfo};
//								}else{
//										labels = new String[] {"statementUuid", "sql","numJobs",
//														"startTimeMs","stopTimeMs","runningJobs"};
//										data = new Object[] {statementUuid,sql,numSinks,startTimeMs,stopTimeMs,runningJobInfo};
//								}
//						}else if(completedCopy!=null && completedCopy.size()>0){
//								CompositeData[] completedJobInfo = getJobInfoCompositeData(jobInfoType, completedCopy);
//								labels = new String[] {"statementUuid", "sql","numJobs",
//												"startTimeMs","stopTimeMs","completedJobs"};
//								data = new Object[] {statementUuid,sql,numSinks,startTimeMs,stopTimeMs,completedJobInfo};
//						}else{
//								labels = new String[] {"statementUuid", "sql","numJobs",
//												"startTimeMs","stopTimeMs"};
//								data = new Object[] {statementUuid,sql,numSinks,startTimeMs,stopTimeMs};
//						}
//						return new CompositeDataSupport(xct,labels,data);
//				} catch (Exception e) {
//						throw new RuntimeException(e);
//				}
//		}
//
//		protected CompositeData[] getJobInfoCompositeData(CompositeType jobInfoType, Set<JobInfo> completedCopy) {
//				int i;
//				CompositeData[] completedJobInfo = new CompositeData[completedCopy.size()];
//				i=0;
//				for(JobInfo info:completedCopy){
//						completedJobInfo[i] = info.toCompositeData(jobInfoType);
//						i++;
//				}
//				return completedJobInfo;
//		}
}

