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

package com.splicemachine.pipeline.writerstatus;

import com.splicemachine.pipeline.api.WriterStatus;
import com.splicemachine.pipeline.client.ActionStatusReporter;

/**
 * @author Scott Fines
 *         Created on: 9/6/13
 */
public class ActionStatusMonitor implements WriterStatus {
    private final ActionStatusReporter statusMonitor;

    public ActionStatusMonitor(ActionStatusReporter statusMonitor) {
        this.statusMonitor = statusMonitor;
    }

    @Override public int getExecutingBufferFlushes() { return statusMonitor.numExecutingFlushes.get(); }
    @Override public long getTotalSubmittedFlushes() { return statusMonitor.totalFlushesSubmitted.get(); }
    @Override public long getFailedBufferFlushes() { return statusMonitor.failedBufferFlushes.get(); }
    @Override public long getNotServingRegionFlushes() { return statusMonitor.notServingRegionFlushes.get(); }
    @Override public long getTimedOutFlushes() { return statusMonitor.timedOutFlushes.get(); }
    @Override public long getGlobalErrors() { return statusMonitor.globalFailures.get(); }
    @Override public long getPartialFailures() { return statusMonitor.partialFailures.get(); }
    @Override public long getMaxFlushTime() { return statusMonitor.maxFlushTime.get(); }
    @Override public long getMinFlushTime() { return statusMonitor.minFlushTime.get(); }
    @Override public long getWrongRegionFlushes() { return statusMonitor.wrongRegionFlushes.get(); }
    @Override public long getMaxFlushedBufferSize() { return statusMonitor.maxFlushSizeBytes.get(); }
    @Override public long getTotalFlushedBufferSize() { return statusMonitor.totalFlushSizeBytes.get(); }
    @Override public long getMinFlushedBufferSize() { return statusMonitor.minFlushSizeBytes.get(); }
    @Override public long getMinFlushedBufferEntries() { return statusMonitor.minFlushEntries.get(); }
    @Override public long getMaxFlushedBufferEntries() { return statusMonitor.maxFlushEntries.get(); }
    @Override public long getTotalFlushedBufferEntries() { return statusMonitor.totalFlushEntries.get(); }
    @Override public long getTotalFlushTime() { return statusMonitor.totalFlushTime.get(); }
    @Override public long getMaxRegionsPerFlush() { return statusMonitor.maxFlushRegions.get(); }
    @Override public long getMinRegionsPerFlush() { return statusMonitor.minFlushRegions.get(); }

    @Override
    public long getAvgRegionsPerFlush() {
        return (long)(statusMonitor.totalFlushRegions.get()/(double)statusMonitor.totalFlushesSubmitted.get());
    }

    @Override
    public double getAvgFlushTime() {
        long totalFlushTime = getTotalFlushTime();
        long totalFlushes = getTotalSubmittedFlushes();
        return (double)totalFlushTime/totalFlushes;
    }

    @Override
    public void reset() {
        statusMonitor.reset();
    }

		@Override public long getTotalRejectedFlushes() { return statusMonitor.rejectedCount.get(); }

		@Override public double getAvgFlushedBufferSize() {
        return statusMonitor.totalFlushSizeBytes.get()/(double)statusMonitor.totalFlushesSubmitted.get();
    }

    @Override
    public double getAvgFlushedBufferEntries() {
        return statusMonitor.totalFlushEntries.get()/(double)statusMonitor.totalFlushesSubmitted.get();
    }

    @Override
    public double getOverallWriteThroughput() {
        /*
         * Throughput in rows/ms
         */
        long flushTimeMs = statusMonitor.totalFlushTime.get();
        double rowsPerMs = statusMonitor.totalFlushEntries.get()/(double)flushTimeMs;

        return rowsPerMs*1000; //throughput in rows/s
    }

    @Override
    public double getAvgFlushedEntriesPerRegion() {
        return getAvgFlushedBufferEntries()/getAvgRegionsPerFlush();
    }

    @Override
    public double getAvgFlushedSizePerRegion() {
        return getAvgFlushedBufferSize()/getAvgRegionsPerFlush();
    }
}
