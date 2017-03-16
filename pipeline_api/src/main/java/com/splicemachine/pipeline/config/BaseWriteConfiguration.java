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
import com.carrotsearch.hppc.IntOpenHashSet;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.api.RecordingContext;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.client.BulkWriteResult;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.concurrent.ExecutionException;

public abstract class BaseWriteConfiguration implements WriteConfiguration {
    private static final Logger LOG = Logger.getLogger(BaseWriteConfiguration.class);

    protected final PipelineExceptionFactory exceptionFactory;
    protected RecordingContext recordingContext;

    public BaseWriteConfiguration(PipelineExceptionFactory exceptionFactory){
        this.exceptionFactory=exceptionFactory;
    }

    @Override
    public RecordingContext getRecordingContext() {
        if (recordingContext == null) {
            recordingContext = new RecordingContext() {
                @Override
                public void recordRead() {
                    // no-op
                }

                @Override
                public void recordFilter() {
                    // no-op
                }

                @Override
                public void recordWrite() {
                    // no-op
                }

                @Override
                public void recordPipelineWrites(long w) {
                    // no-op
                }

                @Override
                public void recordThrownErrorRows(long w) {

                }

                @Override
                public void recordRetriedRows(long w) {

                }

                @Override
                public void recordPartialRows(long w) {

                }

                @Override
                public void recordPartialThrownErrorRows(long w) {

                }

                @Override
                public void recordPartialRetriedRows(long w) {

                }

                @Override
                public void recordPartialIgnoredRows(long w) {

                }

                @Override
                public void recordPartialWrite(long w) {

                }

                @Override
                public void recordIgnoredRows(long w) {

                }

                @Override
                public void recordCatchThrownRows(long w) {

                }

                @Override
                public void recordCatchRetriedRows(long w) {

                }

                @Override
                public void recordRetry(long w) {
                    // no-op
                }

                @Override
                public void recordProduced() {
                    // no-op
                }

                @Override
                public void recordBadRecord(String badRecord, Exception exception) {
                    // no-op
                }

                @Override
                public void recordRegionTooBusy(long w) {
                    // no-op
                }
            };
        }
        return recordingContext;
    }

    @Override
    public void setRecordingContext(RecordingContext recordingContext) {
        this.recordingContext = recordingContext;
    }

    @Override
    public WriteResponse globalError(Throwable t) throws ExecutionException {
        t=exceptionFactory.processPipelineException(t);
        if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            return WriteResponse.IGNORE;
        } else if(exceptionFactory.needsTransactionalRetry(t)){
            return WriteResponse.THROW_ERROR;
        }else if(exceptionFactory.canFinitelyRetry(t)||exceptionFactory.canInfinitelyRetry(t))
            return WriteResponse.RETRY;
        else
            return WriteResponse.THROW_ERROR;
    }

    @Override
    public WriteResponse processGlobalResult(BulkWriteResult bulkWriteResult) throws Throwable {
        WriteResult writeResult = bulkWriteResult.getGlobalResult();
        if (writeResult.isSuccess())
            return WriteResponse.SUCCESS;
        else if (writeResult.isPartial()) {
            IntObjectOpenHashMap<WriteResult> failedRows = bulkWriteResult.getFailedRows();
            if (failedRows != null && failedRows.size() > 0) {
                return WriteResponse.PARTIAL;
            }
            IntOpenHashSet notRun = bulkWriteResult.getNotRunRows();
            if(notRun!=null && notRun.size()>0)
                return WriteResponse.PARTIAL;
            /*
             * We got a partial result, but didn't specify which rows needed behavior.
             * That's weird, but since we weren't told there would be a problem, we may
             * as well ignore
             */
            return WriteResponse.IGNORE;
        } else if (!writeResult.canRetry())
            throw exceptionFactory.processErrorResult(writeResult);
        else
            return WriteResponse.RETRY;
    }

    @Override
    public void registerContext(WriteContext context, ObjectObjectOpenHashMap<KVPair, KVPair> indexToMainMutationMap) {
        SpliceLogUtils.warn(LOG, "registering Context with a base class");
    }

    @Override
    public PipelineExceptionFactory getExceptionFactory(){
        return exceptionFactory;
    }

    @Override
    public boolean skipConflictDetection() {
        return false;
    }
}
