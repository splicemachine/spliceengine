package com.splicemachine.job;

import javax.management.MXBean;

/**
 * @author Scott Fines
 *         Created on: 4/10/13
 */
@MXBean
public interface JobSchedulerManagement {
    public static final String SEP_CHAR = "|";

    public long getTotalSubmittedJobs();

    public long getTotalCompletedJobs();

    public long getTotalFailedJobs();

    public long getTotalCancelledJobs();

    public int getNumRunningJobs();

    /**
     *
     * @return [jobID,statement]
     */
    String[] getRunningJobs();

    /**
     *
     * @return [jobID,taskID,taskStatus]
     */
    String[] getRunningTasks();
}
