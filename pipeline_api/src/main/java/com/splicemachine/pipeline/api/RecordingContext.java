package com.splicemachine.pipeline.api;

/**
 * Record write activity for accumulators.
 */
public interface RecordingContext {
    void recordRead();

    void recordFilter();

    void recordWrite();
    void recordPipelineWrites(long w);
    void recordThrownErrorRows(long w);
    void recordRetriedRows(long w);
    void recordPartialRows(long w);
    void recordPartialThrownErrorRows(long w);
    void recordPartialRetriedRows(long w);
    void recordPartialIgnoredRows(long w);
    void recordPartialWrite(long w);
    void recordIgnoredRows(long w);
    void recordCatchThrownRows(long w);
    void recordCatchRetriedRows(long w);

    void recordRetry(long w);

    void recordProduced();

    void recordBadRecord(String badRecord, Exception exception);

    void recordRegionTooBusy(long w);
}
