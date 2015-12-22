package com.splicemachine.pipeline.config;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.client.BulkWrite;
import com.splicemachine.pipeline.client.BulkWriteResult;

import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 *         Date: 1/30/14
 */
public class ForwardingWriteConfiguration implements WriteConfiguration {
    protected final WriteConfiguration delegate;

    protected ForwardingWriteConfiguration(WriteConfiguration delegate) {
        this.delegate = delegate;
    }

    @Override
    public int getMaximumRetries() {
        return delegate.getMaximumRetries();
    }

    @Override
    public WriteResponse globalError(Throwable t) throws ExecutionException {
        return delegate.globalError(t);
    }

    @Override
    public WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
        return delegate.partialFailure(result, request);
    }

    @Override
    public long getPause() {
        return delegate.getPause();
    }

    @Override
    public void writeComplete(long timeTakenMs, long numRecordsWritten) {
        delegate.writeComplete(timeTakenMs, numRecordsWritten);
    }

    @Override
    public MetricFactory getMetricFactory() {
        return delegate.getMetricFactory();
    }

    @Override
    public WriteResponse processGlobalResult(BulkWriteResult bulkWriteResult) throws Throwable {
        return delegate.processGlobalResult(bulkWriteResult);
    }

    @Override
    public void registerContext(WriteContext context,
                                ObjectObjectOpenHashMap<KVPair, KVPair> indexToMainMutationMap) {
        delegate.registerContext(context, indexToMainMutationMap);
    }

    @Override
    public String toString() {
        return String.format("ForwardingWriteConfiguration{delegate=%s}", delegate);
    }

    @Override
    public PipelineExceptionFactory getExceptionFactory(){
        return delegate.getExceptionFactory();
    }
}
