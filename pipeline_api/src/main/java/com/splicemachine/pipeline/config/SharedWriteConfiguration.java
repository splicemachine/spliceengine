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
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.client.BulkWrite;
import com.splicemachine.pipeline.client.BulkWriteResult;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.utils.Pair;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

public class SharedWriteConfiguration extends BaseWriteConfiguration {

    private static final Logger LOG = Logger.getLogger(SharedWriteConfiguration.class);

    private final List<Pair<WriteContext, ObjectObjectOpenHashMap<KVPair, KVPair>>> sharedMainMutationList = new CopyOnWriteArrayList<>();
    private final AtomicInteger completedCount = new AtomicInteger(0);
    private final int maxRetries;
    private final long pause;

    public SharedWriteConfiguration(int maxRetries,long pause,PipelineExceptionFactory pef){
        super(pef);
        this.maxRetries=maxRetries;
        this.pause=pause;
    }

    @Override
    public void registerContext(WriteContext context, ObjectObjectOpenHashMap<KVPair, KVPair> indexToMainMutationMap) {
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
                List<KVPair> indexMutations = request.mutationsList();
                for (IntObjectCursor<WriteResult> cursor : failedRows) {
                    int row = cursor.key;
                    KVPair kvPair = indexMutations.get(row);
                    WriteResult mutationResult = cursor.value;
                    for (Pair<WriteContext, ObjectObjectOpenHashMap<KVPair, KVPair>> pair : sharedMainMutationList) {
                        KVPair main = pair.getSecond().get(kvPair);
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

