package com.splicemachine.job;

import com.splicemachine.derby.stats.TaskStats;

import java.util.Map;

/**
 * @author Scott Fines
 * Created on: 4/8/13
 */
public interface JobStats {

    int getNumTasks();

    long getTotalTime();

    int getNumSubmittedTasks();

    int getNumCompletedTasks();

    int getNumFailedTasks();

    int getNumInvalidatedTasks();

    int getNumCancelledTasks();

    Map<String,TaskStats> getTaskStats();

    String getJobName();
}
