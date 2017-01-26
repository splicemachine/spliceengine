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

package com.splicemachine.pipeline.config;

import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.RecordingContext;
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
