package com.splicemachine.derby.impl.job.scheduler;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.job.JobStats;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author Scott Fines
 *         Created on: 9/17/13
 */
final class JobStatsAccumulator implements JobStats {
    private Map<String,TaskStats> taskStatsMap = Maps.newConcurrentMap();
    private List<byte[]> failedTasks = Collections.synchronizedList(Lists.<byte[]>newArrayListWithExpectedSize(0));

    private final AtomicInteger tasks = new AtomicInteger(0);
    private final AtomicInteger submittedTasks = new AtomicInteger();

    private final long start = System.nanoTime();

    final AtomicInteger completedTaskCount = new AtomicInteger(0);
    final AtomicInteger invalidTaskCount = new AtomicInteger(0);
    final AtomicInteger cancelledTaskCount = new AtomicInteger(0);
    private final String jobName;

    JobStatsAccumulator(String jobName) {
        this.jobName = jobName;
    }

    void addFailedTask(byte[] taskId){
        this.failedTasks.add(taskId);
    }

    @Override public int getNumTasks() { return tasks.get(); }
    @Override public long getTotalTime() { return System.nanoTime()-start; }
    @Override public int getNumSubmittedTasks() { return submittedTasks.get(); }
    @Override public int getNumCompletedTasks() { return completedTaskCount.get();}
    @Override public int getNumFailedTasks() { return failedTasks.size(); }
    @Override public int getNumInvalidatedTasks() { return invalidTaskCount.get(); }
    @Override public int getNumCancelledTasks() { return cancelledTaskCount.get(); }
    @Override public Map<String, TaskStats> getTaskStats() { return taskStatsMap; }
    @Override public String getJobName() { return jobName; }
    @Override public List<byte[]> getFailedTasks() { return failedTasks; }

    public void addTaskStatus(String taskNode, TaskStats taskStats) {
        taskStatsMap.put(taskNode,taskStats);
    }
}
