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
                public void recordRead(long w) {
                    // no-op
                }

                @Override
                public void recordFilter(long w) {
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

}
