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
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.client.BulkWrite;
import com.splicemachine.pipeline.client.BulkWriteResult;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.storage.Record;
import com.splicemachine.utils.Pair;
import org.apache.log4j.Logger;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class SharedWriteConfiguration extends BaseWriteConfiguration {

    private static final Logger LOG = Logger.getLogger(SharedWriteConfiguration.class);

    private final List<Pair<WriteContext, ObjectObjectOpenHashMap<Record, Record>>> sharedMainMutationList = new CopyOnWriteArrayList<>();
    private final AtomicInteger completedCount = new AtomicInteger(0);
    private final int maxRetries;
    private final long pause;

    public SharedWriteConfiguration(int maxRetries,long pause,PipelineExceptionFactory pef){
        super(pef);
        this.maxRetries=maxRetries;
        this.pause=pause;
    }

    @Override
    public void registerContext(WriteContext context, ObjectObjectOpenHashMap<Record, Record> indexToMainMutationMap) {
        sharedMainMutationList.add(Pair.newPair(context,indexToMainMutationMap));
        completedCount.incrementAndGet();
    }

    @Override
    public int getMaximumRetries() {
        return maxRetries;
    }

    @Override
    public WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
        try {
            IntObjectOpenHashMap<WriteResult> failedRows = result.getFailedRows();
            boolean canRetry = true;
            boolean regionTooBusy = false;
            for (IntObjectCursor<WriteResult> cursor : failedRows) {
                if (!cursor.value.canRetry()) {
                    canRetry = false;
                    break;
                }
                if (cursor.value.getCode() == Code.REGION_TOO_BUSY)
                    regionTooBusy = true;
            }

            if (regionTooBusy) {
                try {
                    Thread.sleep(2 * getPause());
                } catch (InterruptedException e) {
                    LOG.info("Interrupted while waiting due to a RegionTooBusyException", e);
                }
                return WriteResponse.RETRY;
            }
            if (canRetry) {
                return WriteResponse.RETRY;
            }
            else {
                List<Record> indexMutations = request.mutationsList();
                for (IntObjectCursor<WriteResult> cursor : failedRows) {
                    int row = cursor.key;
                    Record kvPair = indexMutations.get(row);
                    WriteResult mutationResult = cursor.value;
                    for (Pair<WriteContext, ObjectObjectOpenHashMap<Record, Record>> pair : sharedMainMutationList) {
                        Record main = pair.getSecond().get(kvPair);
                        WriteContext context = pair.getFirst();
                        // The "main" kvPair from the context may not match the one from the context that failed.
                        // This can happen, for instance, when we have an index update - there's a delete ctx
                        // and an update ctx. Both fail but it may be the constraint check on the update that
                        // caused it.
                        // However, ALL ctxs must NOT be null.
                        assert context != null;
                        context.failed(main, mutationResult);
                    }
                }
                return WriteResponse.IGNORE;
            }
        } catch (Exception e) {
            throw new ExecutionException(e);
        }
    }

    @Override
    public long getPause() {
        return pause;
    }

    @Override
    public void writeComplete(long timeTakenMs, long numRecordsWritten) {
        int remaining = completedCount.decrementAndGet();
        if (remaining <= 0) {
            sharedMainMutationList.clear();
        }
    }

    @Override
    public MetricFactory getMetricFactory() {
        return Metrics.noOpMetricFactory();
    }

    @Override
    public String toString() {
        return "SharedWriteConfiguration{}";
    }

}

