package com.splicemachine.job;

import javax.management.MXBean;

/**
 * @author Scott Fines
 * Created on: 4/5/13
 */
@MXBean(true)
public interface TaskSchedulerManagement {

    int getNumPendingTasks();

    int getCurrentWorkers();

    int getMaxWorkers();

    long getTotalSubmittedTasks();

    long getTotalCompletedTasks();

    long getTotalFailedTasks();

    long getTotalCancelledTasks();

    long getTotalInvalidatedTasks();

    int getNumRunningTasks();

    int getHighestWorkerLoad();

    int getLowestWorkerLoad();

    void setMaxWorkers(int maxWorkers);

}
