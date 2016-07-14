/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
