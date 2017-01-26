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
