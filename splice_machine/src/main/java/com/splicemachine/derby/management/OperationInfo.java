package com.splicemachine.derby.management;

import com.splicemachine.derby.impl.job.JobInfo;

import java.beans.ConstructorProperties;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 *         Date: 1/21/14
 */
public class OperationInfo{
    private final long operationUuid;
    private final String operationTypeName;
    private final boolean isRight;
    private final long parentOperationUuid; //-1 for no parent
    private AtomicInteger numJobs=new AtomicInteger(0);
    private AtomicInteger numTasks=new AtomicInteger(0);
    private long statementId;
    private volatile int numFailedTasks=-1;
    private String info;
    private Set<JobInfo> jobs=new CopyOnWriteArraySet<JobInfo>();

    public OperationInfo(long operationUuid,
                         long statementId,
                         String operationTypeName,
                         String info,
                         boolean isRight,
                         long parentOperationUuid){
        this.operationUuid=operationUuid;
        this.isRight=isRight;
        this.operationTypeName=operationTypeName;
        this.parentOperationUuid=parentOperationUuid;
        this.statementId=statementId;
        this.info=info;
    }

    @ConstructorProperties({"right",
            "numTasks","numJobs",
            "parentOperationUuid","operationTypeName",
            "operationUuid","statementId","numFailedTasks"})
    public OperationInfo(boolean isRight,int numTasks,int numJobs,
                         long parentOperationUuid,String operationTypeName,long operationUuid,
                         long statementUuid,int numFailedTasks){
        this.isRight=isRight;
        this.numJobs.set(numJobs);
        this.numTasks.set(numTasks);
        this.parentOperationUuid=parentOperationUuid;
        this.operationTypeName=operationTypeName;
        this.operationUuid=operationUuid;
        this.statementId=statementUuid;
        this.numFailedTasks=numFailedTasks;
    }

    public int getNumFailedTasks(){
        synchronized(this){
            if(numFailedTasks<0){
                numFailedTasks=0;
                for(JobInfo job : jobs){
                    numFailedTasks+=job.getTasksFailed();
                }
            }
        }
        return numFailedTasks;
    }

    public int getRunningTasks(){
        int numRunning=0;
        for(JobInfo job : jobs){
            numRunning+=job.getRunningTaskCount();
        }
        return numRunning;
    }

    public long getOperationUuid(){ return operationUuid; }
    public String getOperationTypeName(){ return operationTypeName; }
    public long getParentOperationUuid(){ return parentOperationUuid; }
    public int getNumJobs(){ return numJobs.get(); }
    public int getNumTasks(){ return numTasks.get(); }
    public String getInfo(){ return info; }
    public boolean isRight(){ return isRight; }

    public void addJob(JobInfo jobInfo){
        this.numJobs.incrementAndGet();
        this.numTasks.addAndGet(jobInfo.totalTaskCount());
        numFailedTasks=0;
        this.jobs.add(jobInfo);
    }

    @Override
    public boolean equals(Object o){
        if(this==o) return true;
        if(!(o instanceof OperationInfo)) return false;
        OperationInfo that=(OperationInfo)o;

        return operationUuid==that.operationUuid;
    }

    @Override
    public int hashCode(){
        return (int)(operationUuid^(operationUuid>>>32));
    }

    public long getStatementId(){
        return statementId;
    }

}
