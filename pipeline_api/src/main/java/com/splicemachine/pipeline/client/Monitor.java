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

import com.splicemachine.pipeline.callbuffer.BufferConfiguration;
import com.splicemachine.pipeline.api.WriteCoordinatorStatus;

public class Monitor implements WriteCoordinatorStatus,BufferConfiguration{
    public volatile long maxHeapSize;
    public volatile int maxEntries;
    public volatile int maxRetries;
    public volatile int maxFlushesPerRegion;
    public AtomicInteger outstandingBuffers = new AtomicInteger(0);
    public volatile long pauseTime;
    public AtomicLong writesRejected = new AtomicLong(0l);

    public Monitor(long maxHeapSize, int maxEntries, int maxRetries,long pauseTime,int maxFlushesPerRegion) {
        this.maxHeapSize = maxHeapSize;
        this.maxEntries = maxEntries;
        this.maxRetries = maxRetries;
        this.pauseTime = pauseTime;
        this.maxFlushesPerRegion = maxFlushesPerRegion;
    }

    @Override public long getMaxBufferHeapSize() { return maxHeapSize; }
    @Override public void setMaxBufferHeapSize(long newMaxHeapSize) { this.maxHeapSize = newMaxHeapSize; }
    @Override public int getMaxBufferEntries() { return maxEntries; }
    @Override public void setMaxBufferEntries(int newMaxBufferEntries) { this.maxEntries = newMaxBufferEntries; }
    @Override public int getOutstandingCallBuffers() { return outstandingBuffers.get(); }
    @Override public int getMaximumRetries() { return maxRetries; }
    @Override public void setMaximumRetries(int newMaxRetries) { this.maxRetries = newMaxRetries; }
    @Override public long getPauseTime() { return pauseTime; }
    @Override public void setPauseTime(long newPauseTimeMs) { this.pauseTime = newPauseTimeMs; }
    @Override public long getMaxHeapSize() { return maxHeapSize; }
    @Override public int getMaxEntries() { return maxEntries; }
    @Override public int getMaxFlushesPerRegion() { return maxFlushesPerRegion; }
    @Override public void setMaxFlushesPerRegion(int newMaxFlushesPerRegion) { this.maxFlushesPerRegion = newMaxFlushesPerRegion; }

    @Override
    public long getSynchronousFlushCount() {
        return writesRejected.get();
    }

    @Override
    public void writeRejected() {
        this.writesRejected.incrementAndGet();
    }
}

