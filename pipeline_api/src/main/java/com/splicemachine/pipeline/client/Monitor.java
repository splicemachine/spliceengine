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

