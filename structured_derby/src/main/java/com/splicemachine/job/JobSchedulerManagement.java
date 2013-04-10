package com.splicemachine.job;

import javax.management.MXBean;

/**
 * @author Scott Fines
 *         Created on: 4/10/13
 */
@MXBean
public interface JobSchedulerManagement {

    public long getTotalSubmittedJobs();

    public long getTotalCompletedJobs();

    public long getTotalFailedJobs();

    public long getTotalCancelledJobs();

    public int getNumRunningJobs();

}
