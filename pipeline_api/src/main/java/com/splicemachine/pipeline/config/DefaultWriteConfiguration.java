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