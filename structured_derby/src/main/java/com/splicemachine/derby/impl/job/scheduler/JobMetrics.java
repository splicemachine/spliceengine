package com.splicemachine.derby.impl.job.scheduler;

import com.splicemachine.derby.impl.job.coprocessor.CoprocessorJob;
import com.splicemachine.derby.impl.job.operation.OperationJob;
import com.splicemachine.job.JobSchedulerManagement;
import com.splicemachine.job.Status;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

/**
 * @author Scott Fines
 *         Created on: 9/18/13
 */
class JobMetrics implements JobSchedulerManagement{
    final AtomicLong totalSubmittedJobs = new AtomicLong(0l);
    final AtomicLong totalCompletedJobs = new AtomicLong(0l);
    final AtomicLong totalFailedJobs = new AtomicLong(0l);
    final AtomicLong totalCancelledJobs = new AtomicLong(0l);
    final AtomicInteger numRunningJobs = new AtomicInteger(0);

    private Map<String, String> jobSQLMap = new ConcurrentHashMap<String, String>();
    private Map<String, Pair<String,String>> taskJobStatusMap = new ConcurrentHashMap<String, Pair<String,String>>();

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

    @Override
    public long getTotalSubmittedJobs() {
        return totalSubmittedJobs.get();
    }

    @Override
    public long getTotalCompletedJobs() {
        return totalCompletedJobs.get();
    }

    @Override
    public long getTotalFailedJobs() {
        return totalFailedJobs.get();
    }

    @Override
    public long getTotalCancelledJobs() {
        return totalCancelledJobs.get();
    }

    @Override
    public int getNumRunningJobs() {
        return numRunningJobs.get();
    }

    @Override
    public String[] getRunningJobs() {
        // return [jobID,statement]
        List<String> jobs = new ArrayList<String>();
        StringBuilder buf = new StringBuilder();
        for (Map.Entry<String, String> jobSQL : jobSQLMap.entrySet()) {
            buf.append(jobSQL.getKey());
            buf.append(SEP_CHAR);
            buf.append(jobSQL.getValue());
            jobs.add(buf.toString());
            buf.setLength(0);
        }
        return jobs.toArray(new String[0]);
    }

    @Override
    public String[] getRunningTasks() {
        // return [jobID,taskID,taskStatus]
        List<String> tasks = new ArrayList<String>();
        StringBuilder buf = new StringBuilder();
        for (Map.Entry<String, Pair<String,String>> jobTask : taskJobStatusMap.entrySet()) {
            buf.append(jobTask.getValue().getFirst());
            buf.append(SEP_CHAR);
            buf.append(jobTask.getKey());
            buf.append(SEP_CHAR);
            buf.append(jobTask.getValue().getSecond());
            tasks.add(buf.toString());
            buf.setLength(0);
        }
        return tasks.toArray(new String[tasks.size()]);
    }

    public void addJob(CoprocessorJob job) {
        if (job instanceof OperationJob) {
            jobSQLMap.put(job.getJobId(), ((OperationJob)job).getInstructions().getStatement().getSource());
        }
    }

    public void removeJob(String jobID, Status finalState) {
        jobFinished(finalState);
        jobSQLMap.remove(jobID);
    }

    public void updateTask(byte[] taskID, String jobID, String status) {
        Pair<String,String> taskStatus = new Pair<String, String>(jobID,status);
        taskJobStatusMap.put(Bytes.toString(taskID),taskStatus);
    }

    public void removeTask(byte[] taskID) {
        taskJobStatusMap.remove(Bytes.toString(taskID));
    }
}
