package com.splicemachine.derby.impl.storage;

import com.google.common.collect.Lists;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.job.JobStats;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompositeJobStats implements JobStats {
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
    public Map<String, TaskStats> getTaskStats() {
        Map<String,TaskStats> allTaskStats = new HashMap<String,TaskStats>();
        for(JobStats stat:stats){
            allTaskStats.putAll(stat.getTaskStats());
        }
        return allTaskStats;
    }

    @Override
    public String getJobName() {
        return "multiScanJob"; //TODO -sf- use a better name here
    }

    @Override
    public List<String> getFailedTasks() {
        List<String> failedTasks = Lists.newArrayList();
        for(JobStats stat:stats){
            failedTasks.addAll(stat.getFailedTasks());
        }
        return failedTasks;
    }
}
