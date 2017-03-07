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

    boolean skipConflictDetection();
}
