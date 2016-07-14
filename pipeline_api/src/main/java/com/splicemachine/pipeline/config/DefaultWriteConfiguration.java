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

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.client.BulkWrite;
import com.splicemachine.pipeline.client.BulkWriteResult;
import com.splicemachine.pipeline.client.Monitor;
import com.splicemachine.pipeline.client.WriteResult;

import java.util.concurrent.ExecutionException;

public class DefaultWriteConfiguration extends BaseWriteConfiguration {

    private Monitor monitor;

    public DefaultWriteConfiguration(Monitor monitor,PipelineExceptionFactory pef) {
        super(pef);
        this.monitor = monitor;
    }

    @Override
    public int getMaximumRetries() {
        return monitor.getMaximumRetries();
    }

    @Override
    public long getPause() {
        return monitor.getPauseTime();
    }

    @Override
    public void writeComplete(long timeTakenMs, long numRecordsWritten) {
        //no-op
    }

    @Override
    public MetricFactory getMetricFactory() {
        return Metrics.basicMetricFactory();
    }

    @Override
    public WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
        IntObjectOpenHashMap<WriteResult> failedRows = result.getFailedRows();
        for (IntObjectCursor<WriteResult> cursor : failedRows) {
            if (!cursor.value.canRetry())
                return WriteResponse.THROW_ERROR;
        }
        return WriteResponse.RETRY;
    }

    @Override
    public String toString() {
        return "DefaultWriteConfiguration{}";
    }

}