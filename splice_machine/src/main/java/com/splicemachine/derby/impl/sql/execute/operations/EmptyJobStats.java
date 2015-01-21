package com.splicemachine.derby.impl.sql.execute.operations;

import java.util.Collections;
import java.util.List;

import com.splicemachine.derby.stats.TaskStats;
import com.splicemachine.job.JobStats;

public final class EmptyJobStats implements JobStats {
    @Override
    public long getTotalTime() {
        return 0;
    }

    @Override
    public List<TaskStats> getTaskStats() {
        return Collections.emptyList();
    }

    @Override
    public int getNumTasks() {
        return 0;
    }

    @Override
    public int getNumSubmittedTasks() {
        return 0;
    }

    @Override
    public int getNumInvalidatedTasks() {
        return 0;
    }

    @Override
    public int getNumFailedTasks() {
        return 0;
    }

    @Override
    public int getNumCompletedTasks() {
        return 0;
    }

    @Override
    public int getNumCancelledTasks() {
        return 0;
    }

    @Override
    public String getJobName() {
        return "RDD";
    }

    @Override
    public List<byte[]> getFailedTasks() {
        return Collections.emptyList();
    }
}