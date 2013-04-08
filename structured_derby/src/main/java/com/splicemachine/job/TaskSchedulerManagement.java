package com.splicemachine.job;

import javax.management.MXBean;

/**
 * @author Scott Fines
 * Created on: 4/5/13
 */
@MXBean
public interface TaskSchedulerManagement {

    int getNumPendingTasks();

    int getNumWorkers();

    long getTotalSubmittedTasks();

    long getTotalCompletedTasks();

    long getTotalFailedTasks();

    long getTotalCancelledTasks();

    long getTotalInvalidatedTasks();

    int getNumRunningTasks();

    int getHighestWorkerLoad();

    int getLowestWorkerLoad();
}
