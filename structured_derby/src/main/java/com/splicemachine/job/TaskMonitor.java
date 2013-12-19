package com.splicemachine.job;

import javax.management.MXBean;

/**
 * @author Scott Fines
 * Created on: 4/8/13
 */
@MXBean
public interface TaskMonitor {

    String[] getRunningTasks();

    void cancelJob(String jobId);

    String[] getRunningJobs();

    String getStatus(String taskId);
}
