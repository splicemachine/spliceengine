package com.splicemachine.pipeline.api;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.pipeline.impl.BulkWriteResult;

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

}
