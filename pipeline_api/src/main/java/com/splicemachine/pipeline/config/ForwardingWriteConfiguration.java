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

package com.splicemachine.pipeline.config;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.RecordingContext;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.client.BulkWrite;
import com.splicemachine.pipeline.client.BulkWriteResult;
import com.splicemachine.storage.Record;
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
    public RecordingContext getRecordingContext() {
        return delegate.getRecordingContext();
    }

    @Override
    public void setRecordingContext(RecordingContext recordingContext) {
        delegate.setRecordingContext(recordingContext);
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
                                ObjectObjectOpenHashMap<Record, Record> indexToMainMutationMap) {
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
