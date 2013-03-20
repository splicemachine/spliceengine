package com.splicemachine.hbase;

import javax.management.MXBean;

/**
 * @author Scott Fines
 *         Created on: 3/19/13
 */
@MXBean
public interface JMXThreadPool {

    long getThreadKeepAliveTime();

    void setThreadKeepAliveTime(long timeMs);

    int getCorePoolSize();

    void setCorePoolSize(int newCorePoolSize);

    int getCurrentPoolSize();

    int getLargestPoolSize();

    int getMaximumPoolSize();

    void setMaximumPoolSize(int maximumPoolSize);

    int getCurrentlyExecutingThreads();

    /**
     * @return maxPoolSize - currentlyExecutingThreads
     */
    int getCurrentlyAvailableThreads();


    int getPendingTasks();

    long getTotalScheduledTasks();

    long getTotalCompletedTasks();

    long getTotalRejectedTasks();

}
