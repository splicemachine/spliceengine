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
