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

package com.splicemachine.pipeline.client;

import com.splicemachine.metrics.Counter;
import com.splicemachine.metrics.TimeView;
import com.splicemachine.pipeline.api.WriteStats;

/**
 * @author Scott Fines
 *         Date: 2/5/14
 */
public class SimpleWriteStats implements WriteStats {
    private final long writtenCounter;
    private final long retryCounter;
    private final long thrownErrorsRows;
    private final long retriedRows;
    private final long partialRows;
    private final long partialThrownErrorRows;
    private final long partialRetriedRows;
    private final long partialIgnoredRows;
    private final long partialWrite;
    private final long ignoredRows;
    private final long catchThrownRows;
    private final long catchRetriedRows;
    private final long regionTooBusy;

    public SimpleWriteStats(long writtenCounter, long retryCounter, long thrownErrorsRows, long retriedRows, long partialRows, long partialThrownErrorRows, long partialRetriedRows, long partialIgnoredRows, long partialWrite, long ignoredRows, long catchThrownRows, long catchRetriedRows, long regionTooBusy) {
        this.writtenCounter = writtenCounter;
        this.retryCounter = retryCounter;
        this.thrownErrorsRows = thrownErrorsRows;
        this.retriedRows = retriedRows;
        this.partialRows = partialRows;
        this.partialThrownErrorRows = partialThrownErrorRows;
        this.partialRetriedRows = partialRetriedRows;
        this.partialIgnoredRows = partialIgnoredRows;
        this.partialWrite = partialWrite;
        this.ignoredRows = ignoredRows;
        this.catchThrownRows = catchThrownRows;
        this.catchRetriedRows = catchRetriedRows;
        this.regionTooBusy = regionTooBusy;
    }

    @Override
    public long getWrittenCounter() {
        return writtenCounter;
    }

    @Override
    public long getRetryCounter() {
        return retryCounter;
    }

    @Override
    public long getThrownErrorsRows() {
        return thrownErrorsRows;
    }

    @Override
    public long getRetriedRows() {
        return retriedRows;
    }

    @Override
    public long getPartialRows() {
        return partialRows;
    }

    @Override
    public long getPartialThrownErrorRows() {
        return partialThrownErrorRows;
    }

    @Override
    public long getPartialRetriedRows() {
        return partialRetriedRows;
    }

    @Override
    public long getPartialIgnoredRows() {
        return partialIgnoredRows;
    }

    @Override
    public long getPartialWrite() {
        return partialWrite;
    }

    @Override
    public long getIgnoredRows() {
        return ignoredRows;
    }

    @Override
    public long getCatchThrownRows() {
        return catchThrownRows;
    }

    @Override
    public long getCatchRetriedRows() {
        return catchRetriedRows;
    }

    @Override
    public long getRegionTooBusy() {
        return regionTooBusy;
    }

}
