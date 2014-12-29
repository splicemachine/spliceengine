package com.splicemachine.derby.impl.job.scheduler;

import com.splicemachine.job.JobSchedulerManagement;
import com.splicemachine.job.Status;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.util.Pair;

/**
 * @author Scott Fines
 *         Created on: 9/18/13
 */
public class JobMetrics implements JobSchedulerManagement{
    final AtomicLong totalSubmittedJobs = new AtomicLong(0l);
    final AtomicLong totalCompletedJobs = new AtomicLong(0l);
    final AtomicLong totalFailedJobs = new AtomicLong(0l);
    final AtomicLong totalCancelledJobs = new AtomicLong(0l);
    final AtomicInteger numRunningJobs = new AtomicInteger(0);

    private Map<String, String> jobSQLMap = new ConcurrentHashMap<>();
    private Map<String, Pair<String,String>> taskJobStatusMap = new ConcurrentHashMap<>();

    void jobFinished(Status finalState){
        numRunningJobs.decrementAndGet();
        switch (finalState) {
            case FAILED:
                totalFailedJobs.incrementAndGet();
                break;
            case COMPLETED:
                totalCompletedJobs.incrementAndGet();
                break;
            case CANCELLED:
                totalCancelledJobs.incrementAndGet();
                break;
        }
    }

    @Override public long getTotalSubmittedJobs() { return totalSubmittedJobs.get(); }
    @Override public long getTotalCompletedJobs() { return totalCompletedJobs.get(); }
    @Override public long getTotalFailedJobs() { return totalFailedJobs.get(); }
    @Override public long getTotalCancelledJobs() { return totalCancelledJobs.get(); }
    @Override public int getNumRunningJobs() { return numRunningJobs.get(); }

    public void removeJob(String jobID, Status finalState) {
        // TODO: purge this method (like we purged addJob, UpdateTask, etc.),
        // but only after finding out whether the jobFinished invocation
        // should move elsewhere.
        jobFinished(finalState);
        // jobSQLMap.remove(jobID);
    }

}
