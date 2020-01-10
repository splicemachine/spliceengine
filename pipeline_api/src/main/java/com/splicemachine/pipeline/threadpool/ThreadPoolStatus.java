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

package com.splicemachine.pipeline.threadpool;

import javax.management.MXBean;

/**
 * @author Scott Fines
 * Created on: 6/3/13
 */
@MXBean
public interface ThreadPoolStatus {

    int getPendingTaskCount();

    /**
     * @return the number of threads currently executing a task
     */
    int getActiveThreadCount();

    /**
     * @return the number of threads which have been allocated. This is
     * different from {@link #getActiveThreadCount()} in that it also records
     * threads which are alive, but are not currently executing tasks.
     */
    int getCurrentThreadCount();

    long getTotalSubmittedTasks();

    long getTotalFailedTasks();

    long getTotalSuccessfulTasks();

    /**
     * @return the total number of completed tasks. Includes failed and successful.
     */
    long getTotalCompletedTasks();

    /**
     * @return the maximum number of threads available in the pool.
     */
    int getMaxThreadCount();

    void setMaxThreadCount(int newMaxThreadCount);

    /**
     * @return the keepAliveTime for the writerpool, in milliseconds.
     */
    long getThreadKeepAliveTimeMs();

    /**
     * Set the thread keepAliveTime for the writer pool, in milliseconds.
     * @param timeMs the new keepAliveTime for the writer pool. in milliseconds.
     */
    void setThreadKeepAliveTimeMs(long timeMs);

    /**
     * @return the largest size this writer pool has ever had.
     */
    int getLargestThreadCount();

    long getTotalRejectedTasks();


}
