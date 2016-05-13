package com.splicemachine.pipeline.config;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.RecordingContext;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.client.BulkWrite;
import com.splicemachine.pipeline.client.BulkWriteResult;
import com.splicemachine.pipeline.context.WriteContext;

import java.util.concurrent.ExecutionException;

/**
 * Handle BulkWriteResults differently depending on the type of write being performed.
 */
public interface WriteConfiguration {

    int getMaximumRetries();

    WriteResponse processGlobalResult(BulkWriteResult bulkWriteResult) throws Throwable;

    WriteResponse globalError(Throwable t) throws ExecutionException;

    WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException;

    long getPause();

    void writeComplete(long timeTakenMs, long numRecordsWritten);

    MetricFactory getMetricFactory();

    void registerContext(WriteContext context, ObjectObjectOpenHashMap<KVPair, KVPair> indexToMainMutationMap);

    PipelineExceptionFactory getExceptionFactory();

    RecordingContext getRecordingContext();

    void setRecordingContext(RecordingContext recordingContext);
}
