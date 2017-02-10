/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.pipeline.client;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class ActionStatusReporter{
    public final AtomicInteger numExecutingFlushes = new AtomicInteger(0);
    public final AtomicLong totalFlushesSubmitted = new AtomicLong(0l);
    public final AtomicLong failedBufferFlushes = new AtomicLong(0l);
    public final AtomicLong writeConflictBufferFlushes = new AtomicLong(0l);
    public final AtomicLong notServingRegionFlushes = new AtomicLong(0l);
    public final AtomicLong wrongRegionFlushes = new AtomicLong(0l);
    public final AtomicLong timedOutFlushes = new AtomicLong(0l);
    public final AtomicLong globalFailures = new AtomicLong(0l);
    public final AtomicLong partialFailures = new AtomicLong(0l);
    public final AtomicLong maxFlushTime = new AtomicLong(0l);
    public final AtomicLong minFlushTime = new AtomicLong(Long.MAX_VALUE);
    public final AtomicLong maxFlushSizeBytes = new AtomicLong(0l);
    public final AtomicLong minFlushSizeBytes = new AtomicLong(0l);
    public final AtomicLong totalFlushSizeBytes = new AtomicLong(0l);
    public final AtomicLong maxFlushEntries = new AtomicLong(0l);
    public final AtomicLong minFlushEntries = new AtomicLong(0l);
    public final AtomicLong totalFlushEntries = new AtomicLong(0l);
    public final AtomicLong totalFlushTime = new AtomicLong(0l);
    public final AtomicLong rejectedCount = new AtomicLong(0l);

    public final AtomicLong totalFlushRegions = new AtomicLong(0l);
    public final AtomicLong maxFlushRegions = new AtomicLong(0l);
    public final AtomicLong minFlushRegions = new AtomicLong(0l);

    public void reset(){
        totalFlushesSubmitted.set(0);
        failedBufferFlushes.set(0);
        writeConflictBufferFlushes.set(0);
        notServingRegionFlushes.set(0);
        wrongRegionFlushes.set(0);
        timedOutFlushes.set(0);

        globalFailures.set(0);
        partialFailures.set(0);
        maxFlushTime.set(0);
        minFlushTime.set(0);
        totalFlushTime.set(0);

        maxFlushSizeBytes.set(0);
        minFlushEntries.set(0);
        totalFlushSizeBytes.set(0);

        maxFlushEntries.set(0);
        minFlushEntries.set(0);
        totalFlushEntries.set(0);

        totalFlushRegions.set(0);
        maxFlushRegions.set(0);
        minFlushRegions.set(0);
    }

    public void complete(long timeTakenMs) {
        totalFlushTime.addAndGet(timeTakenMs);
        numExecutingFlushes.decrementAndGet();
    }
}
