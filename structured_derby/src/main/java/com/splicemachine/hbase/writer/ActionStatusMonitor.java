package com.splicemachine.hbase.writer;

/**
 * @author Scott Fines
 *         Created on: 9/6/13
 */
public class ActionStatusMonitor implements WriterStatus {
    private final BulkWriteAction.ActionStatusReporter statusMonitor;

    public ActionStatusMonitor(BulkWriteAction.ActionStatusReporter statusMonitor) {
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

    @Override public double getAvgFlushedBufferSize() {
        return statusMonitor.totalFlushSizeBytes.get()/(double)statusMonitor.totalFlushesSubmitted.get();
    }

    @Override
    public double getAvgFlushedBufferEntries() {
        return statusMonitor.totalFlushEntries.get()/(double)statusMonitor.totalFlushesSubmitted.get();
    }
}
