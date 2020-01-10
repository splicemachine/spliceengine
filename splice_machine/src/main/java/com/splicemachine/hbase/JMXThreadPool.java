/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
