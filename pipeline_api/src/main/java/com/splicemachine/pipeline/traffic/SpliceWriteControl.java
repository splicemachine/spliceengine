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

package com.splicemachine.pipeline.traffic;

/**
 * WriteControl limits (or controls) the rate of writes per region server.  It restricts writes based on the number of
 * writes that are currently "in flight" and the number of writer threads that are currently in use.
 * WriteControl is essentially a multi-variable counting semaphore where the counting variables are the number of
 * current writes and the number of current writer threads.  The limiting variables (or buckets) or further subdivided
 * into independent and dependent writes.  Independent writes being writes to a single table and dependent writes being
 * writes that require multiple tables to written to such as a base table and its indexes.
 *
 * WriteControl does NOT actually perform writes.  It just controls whether or not the write is allowed to proceed.
 * It essentially gives out "permits" when the write request fits within the control limits and rejects write requests when they don't.

 * @author Scott Fines
 *         Date: 1/15/16
 */
public interface SpliceWriteControl{
    enum Status {
        DEPENDENT, INDEPENDENT,REJECTED
    }

    /**
     * tries to register a new dependent or independent write.
     * @sa registerDependentWrite, registerIndependentWrite
     */
    default Status registerWrite(int writes, boolean dependent) {
        return dependent ? registerDependentWrite(writes) : registerIndependentWrite(writes);
    }

    /**
     * tries to register a new dependent write.
     * checks if limits allow another `1` thread and `writes` writes.
     * if not, returns Status.REJECTED (pipeline is too busy)
     */
    Status registerDependentWrite(int writes);

    void registerDependentWriteFinish(int writes);

    /**
     * like @sa registerDependentWrite, but for independent writes
     */
    Status registerIndependentWrite(int writes);
    void registerIndependentWriteFinish(int writes);

    /**
     * @return a WriteStatus struct containing the number of current dependent/independent writes counts and threads
     */
    WriteStatus getWriteStatus();

    int maxDependendentWriteThreads();

    int maxIndependentWriteThreads();

    int maxDependentWriteCount();

    int maxIndependentWriteCount();

    void setMaxIndependentWriteThreads(int newMaxIndependentWriteThreads);

    void setMaxDependentWriteThreads(int newMaxDependentWriteThreads);

    void setMaxIndependentWriteCount(int newMaxIndependentWriteCount);

    void setMaxDependentWriteCount(int newMaxDependentWriteCount);
}
