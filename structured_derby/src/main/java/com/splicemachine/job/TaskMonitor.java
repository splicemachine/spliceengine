package com.splicemachine.job;

import javax.management.MXBean;
import java.util.Collection;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 4/8/13
 */
@MXBean
public interface TaskMonitor {

    List<String> getRunningTasks();

    void cancelJob(String jobId);

    List<String> getRunningJobs();

    String getStatus(String taskId);
}
